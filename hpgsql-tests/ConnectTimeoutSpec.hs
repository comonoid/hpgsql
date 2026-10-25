module ConnectTimeoutSpec where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar, tryReadMVar)
import Control.Exception (IOException, SomeException, bracket, finally, try)
import Control.Monad (forM_, unless, void, when)
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import Data.Time (DiffTime)
import Data.Word (Word16, Word8)
import GHC.Clock (getMonotonicTime)
import Hpgsql (HPgConnection, IrrecoverableHpgsqlError (..))
import Hpgsql.Connection (closeForcefully, connect, connectOpts, connectionIsClosed, defaultConnectOpts, withConnection)
import Hpgsql.Internal.InternalTypes (ConnectOpts (..), ConnectionString (..))
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import System.Timeout (timeout)
import Test.Hspec

spec :: Spec
spec = do
  describe "Connection timeouts" $ parallel $ do
    it
      "Times out if the server accepts but never answers the startup message"
      connectingToSilentServerTimesOut
    it
      "Times out if the server stalls during authentication"
      connectingToServerStallingDuringAuthenticationTimesOut
    it
      "Times out if the server stalls during the type info cache refresh"
      connectingToServerStallingDuringTypeInfoCacheTimesOut
    it
      "Times out through withConnection as well"
      connectingWithConnectionTimesOut
    it
      "Waits indefinitely with a zero timeout"
      connectingWithZeroTimeoutWaitsIndefinitely
    it
      "Waits indefinitely with a negative timeout"
      connectingWithNegativeTimeoutWaitsIndefinitely
    it
      "Does not wait for the type info cache when it is disabled"
      connectingWithTypeInfoCacheDisabledDoesNotWaitForTheCache
    it
      "Times out with the whole budget when the server trickles handshake bytes"
      connectingToServerThatTricklesBytesTimesOutWithTheWholeBudget
    it
      "Fails fast when the connection is refused, without reporting a timeout"
      connectingToARefusedPortFailsWithoutATimeout

-- | The timeout handed to hpgsql in these tests. It is long enough that a busy
-- CI scheduler cannot make a fake server miss its phase.
testConnectTimeout :: DiffTime
testConnectTimeout = 2

-- | Guards that make a regressed (hanging) connect fail the test instead of
-- hanging the whole suite.
connectGuardMicros :: Int
connectGuardMicros = 10_000_000

-- | How long we wait for the fake server to signal that it reached a phase or
-- that the client closed the connection.
waitMicros :: Int
waitMicros = 5_000_000

connectingToSilentServerTimesOut :: IO ()
connectingToSilentServerTimesOut =
  withStallingServer silentServerScript $ \port _phaseReached clientClosed -> do
    connectShouldTimeOut (stallingServerConnInfo port)
    waitForClientClosed clientClosed

connectingToServerStallingDuringAuthenticationTimesOut :: IO ()
connectingToServerStallingDuringAuthenticationTimesOut =
  withStallingServer authenticationPhaseScript $ \port phaseReached clientClosed -> do
    connectShouldTimeOut (stallingServerConnInfo port)
    waitForPhase phaseReached
    waitForClientClosed clientClosed

connectingToServerStallingDuringTypeInfoCacheTimesOut :: IO ()
connectingToServerStallingDuringTypeInfoCacheTimesOut =
  withStallingServer typeInfoCachePhaseScript $ \port phaseReached clientClosed -> do
    connectShouldTimeOut (stallingServerConnInfo port)
    waitForPhase phaseReached
    waitForClientClosed clientClosed

connectingWithConnectionTimesOut :: IO ()
connectingWithConnectionTimesOut =
  withStallingServer silentServerScript $ \port _phaseReached clientClosed -> do
    shouldTimeOutWithin
      10
      (const (pure ()))
      (try (withConnection (stallingServerConnInfo port) testConnectTimeout (\_ -> pure ())) :: IO (Either IrrecoverableHpgsqlError ()))
    waitForClientClosed clientClosed

connectingWithZeroTimeoutWaitsIndefinitely :: IO ()
connectingWithZeroTimeoutWaitsIndefinitely =
  withStallingServer silentServerScript $ \port _phaseReached clientClosed -> do
    result <- timeout 1_000_000 (connect (stallingServerConnInfo port) 0)
    assertStillBlocked "zero" result
    waitForClientClosed clientClosed

connectingWithNegativeTimeoutWaitsIndefinitely :: IO ()
connectingWithNegativeTimeoutWaitsIndefinitely =
  withStallingServer silentServerScript $ \port _phaseReached clientClosed -> do
    result <- timeout 1_000_000 (connect (stallingServerConnInfo port) (-1))
    assertStillBlocked "negative" result
    waitForClientClosed clientClosed

connectingWithTypeInfoCacheDisabledDoesNotWaitForTheCache :: IO ()
connectingWithTypeInfoCacheDisabledDoesNotWaitForTheCache =
  withStallingServer handshakeCompletedScript $ \port _phaseReached clientClosed -> do
    bracket
      (connectOpts defaultConnectOpts {fillTypeInfoCache = False} (stallingServerConnInfo port) testConnectTimeout)
      closeForcefully
      $ \conn -> connectionIsClosed conn `shouldReturn` False
    waitForClientClosed clientClosed

connectingToServerThatTricklesBytesTimesOutWithTheWholeBudget :: IO ()
connectingToServerThatTricklesBytesTimesOutWithTheWholeBudget =
  withStallingServer trickleAuthenticationScript $ \port phaseReached _clientClosed -> do
    -- A per-read timeout would only fire after the whole trickled message
    -- (2.4s) plus another full timeout, so the upper bound distinguishes it
    -- from a single total budget.
    connectShouldTimeOutWithin 3.5 (stallingServerConnInfo port)
    waitForPhase phaseReached

connectingToARefusedPortFailsWithoutATimeout :: IO ()
connectingToARefusedPortFailsWithoutATimeout =
  withUnusedPort $ \port -> do
    result <-
      timeout
        waitMicros
        (try (connect (stallingServerConnInfo port) testConnectTimeout) :: IO (Either IrrecoverableHpgsqlError HPgConnection))
    case result of
      Nothing -> expectationFailure "connect to a refused port did not fail fast"
      Just (Left err) -> err `shouldSatisfy` (not . isConnectTimeout)
      Just (Right conn) -> closeForcefully conn >> expectationFailure "connected to a port that is not listening"

assertStillBlocked :: String -> Maybe HPgConnection -> IO ()
assertStillBlocked label result =
  case result of
    Nothing -> pure ()
    Just conn -> closeForcefully conn >> expectationFailure ("connect with a " ++ label ++ " timeout returned")

connectShouldTimeOut :: ConnectionString -> Expectation
connectShouldTimeOut = connectShouldTimeOutWithin 10

connectShouldTimeOutWithin :: Double -> ConnectionString -> Expectation
connectShouldTimeOutWithin upperBound connstr =
  shouldTimeOutWithin
    upperBound
    closeForcefully
    (try (connect connstr testConnectTimeout) :: IO (Either IrrecoverableHpgsqlError HPgConnection))

-- | Runs a connection attempt that is expected to fail with the connect
-- timeout, asserting that it fails within the expected time. `cleanup` is used
-- if the attempt unexpectedly succeeds, so the test does not leak a connection.
shouldTimeOutWithin :: Double -> (a -> IO ()) -> IO (Either IrrecoverableHpgsqlError a) -> Expectation
shouldTimeOutWithin upperBound cleanup action = do
  start <- getMonotonicTime
  result <- timeout connectGuardMicros action
  end <- getMonotonicTime
  case result of
    Nothing -> expectationFailure "the connection attempt did not time out; it is still blocked"
    Just (Left err) -> do
      err `shouldSatisfy` isConnectTimeout
      let elapsed = end - start
      elapsed `shouldSatisfy` (\e -> e > 0.5 && e < upperBound)
    Just (Right value) -> cleanup value >> expectationFailure "the connection attempt succeeded unexpectedly"

waitForPhase :: MVar () -> Expectation
waitForPhase phaseReached = do
  reached <- timeout waitMicros (takeMVar phaseReached)
  reached `shouldBe` Just ()

waitForClientClosed :: MVar () -> Expectation
waitForClientClosed clientClosed = do
  closed <- timeout waitMicros (takeMVar clientClosed)
  closed `shouldBe` Just ()

isConnectTimeout :: IrrecoverableHpgsqlError -> Bool
isConnectTimeout IrrecoverableHpgsqlError {hpgsqlDetails} =
  "Could not connect in the supplied timeout" `Text.isInfixOf` hpgsqlDetails

stallingServerConnInfo :: Word16 -> ConnectionString
stallingServerConnInfo port =
  ConnectionString
    { hostname = "127.0.0.1",
      port,
      user = "postgres",
      password = "postgres",
      database = "postgres",
      options = ""
    }

-- | Starts a server that accepts a single connection, consumes hpgsql's startup
-- message, runs the supplied script, and then reads until the client closes the
-- connection. The script receives an 'MVar' it can fill once it has observed the
-- client reaching the intended protocol phase, and the action receives another
-- 'MVar' that is filled once the client has closed the connection, so tests can
-- verify that timed out connections are cleaned up.
withStallingServer ::
  (NS.Socket -> MVar () -> IO ()) ->
  (Word16 -> MVar () -> MVar () -> IO a) ->
  IO a
withStallingServer script action = NS.withSocketsDo $ do
  listeningSocket <- NS.socket NS.AF_INET NS.Stream 0
  NS.setSocketOption listeningSocket NS.ReuseAddr 1
  NS.bind listeningSocket (NS.SockAddrInet 0 (NS.tupleToHostAddress (127, 0, 0, 1)))
  NS.listen listeningSocket 1
  port <- fromIntegral <$> NS.socketPort listeningSocket :: IO Word16
  phaseReached <- newEmptyMVar
  clientClosed <- newEmptyMVar
  acceptedConn <- newEmptyMVar
  serverThread <-
    forkIO $
      ignoreServerThreadExceptions $
        bracket (fst <$> NS.accept listeningSocket) NS.close $ \conn -> do
          putMVar acceptedConn conn
          void $ NSB.recv conn 1024 -- Consume the startup message.
          script conn phaseReached
          drainUntilEof conn
          putMVar clientClosed ()
  let stopServer = do
        -- Shut the accepted socket down before killing the server thread: the
        -- thread may be blocked in `recv`, and `killThread` cannot interrupt a
        -- blocking FFI call. The timeout on `killThread` keeps a broken test
        -- from hanging the whole suite.
        mConn <- tryReadMVar acceptedConn
        forM_ mConn $ \conn ->
          void (try (NS.shutdown conn NS.ShutdownBoth) :: IO (Either IOException ()))
        NS.close listeningSocket
        void $ timeout 1_000_000 (killThread serverThread)
  action port phaseReached clientClosed `finally` stopServer

-- | Socket operations in the fake server can fail when a test (or the cleanup
-- below) closes the connection. The test's assertions, not the server thread,
-- decide whether the test passed, so these exceptions are ignored.
ignoreServerThreadExceptions :: IO () -> IO ()
ignoreServerThreadExceptions action = void (try action :: IO (Either SomeException ()))

-- | Reads and discards data until the client closes the connection. A reset
-- (rather than a clean shutdown) also counts as closed.
drainUntilEof :: NS.Socket -> IO ()
drainUntilEof conn = do
  received <- try (NSB.recv conn 4096) :: IO (Either IOException BS.ByteString)
  case received of
    Left _ -> pure ()
    Right bytes -> unless (BS.null bytes) (drainUntilEof conn)

silentServerScript :: NS.Socket -> MVar () -> IO ()
silentServerScript _conn _phaseReached = pure ()

authenticationPhaseScript :: NS.Socket -> MVar () -> IO ()
authenticationPhaseScript conn phaseReached = do
  -- AuthenticationCleartextPassword: 'R', length 8, auth method 3.
  NSB.sendAll conn (BS.pack [0x52, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x03])
  -- hpgsql answers with a PasswordMessage ('p'); we never answer it. Receiving
  -- it proves that the connection attempt reached the authentication phase.
  passwordMessage <- NSB.recv conn 4096
  when (hasMessageType [0x70] passwordMessage) $ putMVar phaseReached ()

typeInfoCachePhaseScript :: NS.Socket -> MVar () -> IO ()
typeInfoCachePhaseScript conn phaseReached = do
  NSB.sendAll conn handshakeCompletedMessages
  -- hpgsql now refreshes its type info cache; never answer that query. A Parse
  -- ('P', extended query) or Query ('Q', simple query) proves that the
  -- connection attempt reached the cache phase.
  cacheQuery <- NSB.recv conn 4096
  when (hasMessageType [0x50, 0x51] cacheQuery) $ putMVar phaseReached ()

handshakeCompletedScript :: NS.Socket -> MVar () -> IO ()
handshakeCompletedScript conn _phaseReached = NSB.sendAll conn handshakeCompletedMessages

-- | Sends an AuthenticationOk message one byte at a time. The phase is
-- signalled after the fourth byte, so the test cannot pass if the client timed
-- out before the server trickled any bytes to it.
trickleAuthenticationScript :: NS.Socket -> MVar () -> IO ()
trickleAuthenticationScript conn phaseReached =
  forM_ (zip [1 :: Int ..] [0x52, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00]) $ \(index, byte) -> do
    NSB.sendAll conn (BS.singleton byte)
    when (index == 4) $ putMVar phaseReached ()
    threadDelay 300_000

-- | Checks that the first byte of a received PostgreSQL message is one of the
-- expected message types.
hasMessageType :: [Word8] -> BS.ByteString -> Bool
hasMessageType expected bytes =
  not (BS.null bytes) && BS.head bytes `elem` expected

handshakeCompletedMessages :: BS.ByteString
handshakeCompletedMessages =
  BS.pack
    ( [0x52, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00] -- AuthenticationOk
        ++ [0x4B, 0x00, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x04, 0xD2, 0x00, 0x00, 0x16, 0x2E] -- BackendKeyData
        ++ [0x5A, 0x00, 0x00, 0x00, 0x05, 0x49] -- ReadyForQuery (idle)
    )

-- | Binds a port without listening on it, so connecting to it is refused while
-- the socket is held. This avoids racing another process for a freed port.
withUnusedPort :: (Word16 -> IO a) -> IO a
withUnusedPort action = NS.withSocketsDo $
  bracket (NS.socket NS.AF_INET NS.Stream 0) NS.close $ \sock -> do
    NS.bind sock (NS.SockAddrInet 0 (NS.tupleToHostAddress (127, 0, 0, 1)))
    port <- fromIntegral <$> NS.socketPort sock :: IO Word16
    action port
