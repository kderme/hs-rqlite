{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Rqlite
    ( --posts
      PostResult(..)
    , postQueries
    , postQuery
      -- gets
    , GetResult(..)
    , getQuery
    , Level(..)
      -- exceptions
    , RQliteError(..)
    , reify
    ) where

import           Control.Exception
import           Data.Aeson hiding (Result)
import           Data.List (find, intercalate)
import           Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.ByteString.Char8 as Char8
import           Data.Scientific
import           Data.Typeable
import qualified Data.HashMap.Strict as M
import           GHC.Generics
import           GHC.IO.Exception
import           Network.HTTP hiding (host)
import           Network.Stream

data RQResult a =
      RQResults { results :: [a]}
    | RQLeaderError Text
    deriving (Show, Read, Generic)

instance FromJSON a => FromJSON (RQResult a) where
    parseJSON j = do
        Object o <- parseJSON j
        case M.toList (o :: Object) of
            [("results", x)] -> do
                ls <- parseJSON x
                return $ RQResults ls
            [("error", String err)] | Text.isPrefixOf "leadership lost"  err ->
                return $ RQLeaderError err
            _ -> throw $ UnexpectedResponse $ concat
                ["Failed to decode ", show j]

-- Post Requests --

data PostResult
    = PostResult { last_insert_id :: Int }
    | EmptyPostResult
    | PostError Text -- this indicates an SQlite error, returned by rqlite.
    deriving (Show, Read, Generic)

instance FromJSON PostResult where
    parseJSON j = do
        Object o <- parseJSON j
        case M.toList (o :: Object) of
            [("rows_affected", _), ("last_insert_id", Number n)] ->
                return $ PostResult $ base10Exponent n
            [("last_insert_id", Number n)] -> -- this happens when deleting
                return $ PostResult $ base10Exponent n
            [("error", String txt)] ->
                return $ PostError txt
            [] -> -- this happens when creating table
                return EmptyPostResult
            _ -> throw $ UnexpectedResponse $ concat
                    ["Failed to decode ", show j, " as PostResult"]

post :: String -> String -> IO (Either (Response String) String)
post request body =
    reifyRed $ simpleHTTP $ postRequestWithBody
        request
        "application/json"
        body

postQueries :: Bool -> String -> [String] -> IO [PostResult]
postQueries redirect host queries = do
    let body = concat
            [ "["
            , intercalate "," (fmap (\str -> concat [" \"", str, "\"  "]) queries)
            , "]"
            ]
        go :: Int -> String -> [Response String] -> IO [PostResult]
        go 5 _ acc = throwIO $ MaxNumberOfRedirections $ reverse acc
        go n req acc = do
            mResp <- post req body
            case mResp of
                Right resp -> do
                    let postResults = getLastInsertId resp
                    if length postResults /= length queries
                    then throw $ UnexpectedResponse $ concat
                            ["Posted ", show (length queries), " queries, but got ", show (length postResults), " results"]
                    else return postResults
                Left resp ->
                    if redirect
                    then case find isLocation (rspHeaders resp) of
                        Nothing            -> throwIO $ FailedRedirection resp
                        Just (Header _ q') ->
                            go (n + 1) q' (resp : acc)
                    else throwIO $ HttpRedirect resp
    go 0 (mkPostRequest host) []

mkPostRequest :: String -> String
mkPostRequest host = "http://" ++ host ++ "/db/execute?pretty"

-- | This can be used to insert, create, delete a table..
postQuery :: Bool -> String -> String -> IO PostResult
postQuery redirect host body = head <$>
    postQueries redirect host [body]

getLastInsertId :: String -> [PostResult]
getLastInsertId str = case eitherDecodeStrict $ Char8.pack $ str of
        Left e -> throw $ UnexpectedResponse $ concat
            ["Got ", e, " while trying to decode ", str, " as PostResult"]
        Right (RQResults res)     -> res
        Right (RQLeaderError err) -> throw $ LeadershipLost err

-- Get Requests --

data GetResult a =
      GetResult [a]
    | GetError String
    deriving (Show, Read, Generic)

data Level = None | Weak | Strong
        deriving (Show, Eq, Generic)

instance FromJSON a => FromJSON (GetResult a) where
    parseJSON j = do
        Object o <- parseJSON j
        case M.toList (o :: Object) of
            [("values", v), ("types", _), ("columns", _)] ->
                GetResult <$> parseJSON v
            [("types", _), ("columns", _)] ->
                return $ GetResult [] -- when there is no element
            [("error", String str)] ->
                return $ GetError $ Text.unpack str
            _ -> throw $ UnexpectedResponse $ concat
                ["Failed to decode ", show j, " as GetResult"]

mkQuery :: String -> Maybe Level -> String -> String
mkQuery host level q = concat
        [ "http://"
        , host
        , "/db/query?"
        , encodeLevel level
        , "pretty&q="
        , urlEncode q
        ]

-- | This can be used to query a table.
getQuery :: forall a. FromJSON a => Maybe Level -> String -> Bool -> String -> IO (GetResult a)
getQuery level host redirect q = go 0 (mkQuery host level q) []
    where
        go :: Int -> String -> [Response String] -> IO (GetResult a)
        go 5 _ acc = throwIO $ MaxNumberOfRedirections $ reverse acc
        go n query acc = do
            let http = simpleHTTP $ getRequest query
            mResp <- if redirect
                then reifyRed http
                else Right <$> reify http
            case mResp of
                Right respBody ->
                    case eitherDecodeStrict $ Char8.pack respBody of
                    Left e -> throwIO $ UnexpectedResponse $ concat
                        ["Got ", e, " while trying to decode ", respBody, " as GetResult"]
                    Right (RQResults res)     -> return $ head res
                    Right (RQLeaderError err) -> throwIO $ LeadershipLost err
                Left resp ->
                    case find isLocation (rspHeaders resp) of
                        Nothing            -> throwIO $ FailedRedirection resp
                        Just (Header _ q') ->
                            go (n + 1) q' (resp : acc)

isLocation :: Header -> Bool
isLocation (Header HdrLocation _) = True
isLocation _                      = False

encodeLevel :: Maybe Level -> String
encodeLevel Nothing       = ""
encodeLevel (Just None)   = "level=none&"
encodeLevel (Just Weak)   = "level=weak&"
encodeLevel (Just Strong) = "level=strong&"

-- Exeptions handling

reify ::IO (Result (Response String)) -> IO String
reify = reifyHTTPErrors . reifyStreamErrors . reifyNoSuchThing

-- | Like reify, but returns Left for Redirect errors, instead of throwing them.
reifyRed :: IO (Result (Response String)) -> IO (Either (Response String) String)
reifyRed = reifyHTTPErrorsRed . reifyStreamErrors . reifyNoSuchThing

reifyStreamErrors :: IO (Result a) -> IO a
reifyStreamErrors action = do
    res <- action
    case res of
        Left err -> throwIO $ StreamError err
        Right a -> return a

-- | Like reifyHTTPErrors, but returns Left for Redirect errors, instead of throwing them.
reifyHTTPErrorsRed :: IO (Response String) -> IO (Either (Response String) String)
reifyHTTPErrorsRed action = do
    resp <-action
    case rspCode resp of
        (2,0,0) -> return $ Right $ rspBody resp
        (3,_,_) -> return $ Left resp
        (5,0,3) | Text.isPrefixOf "not leader"  (Text.pack $ rspBody resp) -> throwIO NotLeader
        _       -> throwIO $ HttpError resp

reifyHTTPErrors :: IO (Response String) -> IO String
reifyHTTPErrors action = do
    mresp <- reifyHTTPErrorsRed action
    case mresp of
        Left resp -> throwIO $ HttpRedirect resp
        Right str -> return str

reifyNoSuchThing :: IO a -> IO a
reifyNoSuchThing action = do
    a <- try action
    case a of
        Right a' -> return a'
        Left (e :: IOError) | ioe_type e == NoSuchThing -> throwIO $ NodeUnreachable e 1
        Left e -> throwIO e

data RQliteError =
      NodeUnreachable IOError Int -- Int here indicates number of trials we did.
    | StreamError ConnError
    | HttpError (Response String) -- Does the user really need the whole response here?
    | HttpRedirect (Response String)
                     -- since RQlite is a distributed db, redirections to the leader
                     -- deserve a different constructor, even though technically it
                     -- is just another http error code.
    | MaxNumberOfRedirections [Response String]
    | FailedRedirection (Response String)
    | LeadershipLost Text
    | NotLeader
    | UnexpectedResponse String
    deriving (Show, Typeable, Exception)
