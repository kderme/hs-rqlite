{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}

module Rqlite.Status
    ( RQStatus (..)
    , getLeader
    , retryUntilAlive
    , queryStatus
    ) where

import           Control.Concurrent (threadDelay)
import           Control.Exception
import           Data.Aeson hiding (Result)
import qualified Data.ByteString.Char8 as C8
import           GHC.Generics
import           Network.HTTP hiding (host)

import           Rqlite

-- This module provides support for requesting the status of a node
-- The actual status has many more info than what @RQStatus@ contains.

queryStatus :: String -> IO RQStatus
queryStatus host = do
    resp <- reify $ simpleHTTP $ getRequest $ concat
            [ "http://"
            , host
            , "/status?pretty"
            ]
    case eitherDecodeStrict $ C8.pack $ resp of
            Left e -> throwIO $ UnexpectedResponse $ concat
                ["Got ", e, " while trying to decode ", resp, " as PostResult"]
            Right st -> return st

data RQState = Leader | Follower | UnknownState
    deriving (Show, Eq, Generic)

readState :: String -> RQState
readState "Leader"   = Leader
readState "Follower" = Follower
readState _          = UnknownState

-- | A subset of the status that a node reports.
data RQStatus = RQStatus {
      path           :: String
    , leader         :: Maybe String
    , peers          :: [String]
    , state          :: RQState
    , fk_constraints :: Bool
    } deriving (Show, Eq, Generic)

instance FromJSON RQStatus where
    parseJSON j = do
        Object o <- parseJSON j
        Object store <- o .: "store"
        pth <- store .: "dir"
        ldr <- store .: "leader"
        let mLeader = if ldr == "" then Nothing else Just ldr
        prs :: [String] <- store .: "peers"
        sqliteInfo <- store .: "sqlite3"
        raft <- store .: "raft"
        stStr <- raft .: "state"
        let st = readState stStr
        fk' :: String <- sqliteInfo .: "fk_constraints"
        let fk = fk' /= "disabled"
        return $ RQStatus pth mLeader prs st fk

getLeader :: String -> IO (Maybe String)
getLeader host = do
    mstatus <- queryStatus host
    return $ leader mstatus

-- | This can be used to make sure that a node is alive, before starting to query it.
retryUntilAlive :: String -> IO ()
retryUntilAlive host = go 40
    where
        go :: Int -> IO ()
        go n = do
            mStatus <- try $ queryStatus host
            case mStatus of
                Right _ -> return ()
                Left (NodeUnreachable e _) -> do
                    putStrLn $ "Warning: Got " ++ show e ++ " while trying to get Status from " ++ host ++ ". Trying again.."
                    threadDelay 500000
                    if n > 0 then go $ n - 1
                    else throwIO $ NodeUnreachable e 40
                Left e -> throwIO e
