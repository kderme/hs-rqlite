{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
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
import qualified Data.Text
import           Data.Bifunctor
import qualified Data.ByteString
import qualified Data.ByteString.Char8
import           Data.Scientific
import           Data.Typeable
import qualified Data.HashMap.Strict as M
import           GHC.Generics
import           Network.HTTP
import           Network.Stream

data RQResult a = RQResult {
    results :: [a]
    } deriving (Show, Read, Generic, ToJSON, FromJSON)

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
post request body = do
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
                    case find isLocation (rspHeaders resp) of
                        Nothing            -> throwIO $ FailedRedirection resp
                        Just (Header _ q') -> do
                            putStrLn $ "Warning: Redirected to " ++ q'
                            go (n + 1) q' (resp : acc)
    go 0 (mkPostRequest host) []

mkPostRequest :: String -> String
mkPostRequest host = "http://" ++ host ++ "/db/execute?pretty"

-- | This can be used to insert, create, delete a table..
postQuery :: Bool -> String -> String -> IO PostResult
postQuery redirect host body = head <$>
    postQueries redirect host [body]

getLastInsertId :: String -> [PostResult]
getLastInsertId str = case eitherDecodeStrict $ Data.ByteString.Char8.pack $ str of
        Left e -> throw $ UnexpectedResponse $ concat
            ["Got ", e, " while trying to decode ", str, " as PostResult"]
        Right (a :: (RQResult PostResult)) -> results a

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
                return $ GetError $ Data.Text.unpack str
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
getQuery :: FromJSON a => Maybe Level -> String -> Bool -> String -> IO (GetResult a)
getQuery level host redirect q = go 0 (mkQuery host level q) []
    where
        go 5 _ acc = throwIO $ MaxNumberOfRedirections $ reverse acc
        go n query acc = do
            let http = simpleHTTP $ getRequest query
            mResp <- if redirect
                then reifyRed http
                else Right <$> reify http
            case mResp of
                Right respBody ->
                    case eitherDecodeStrict $ Data.ByteString.Char8.pack respBody of
                    Left e -> throwIO $ UnexpectedResponse $ concat
                        ["Got ", e, " while trying to decode ", respBody, " as GetResult"]
                    Right (a :: RQResult (GetResult a)) -> return $ head $ results a
                Left resp -> do
                    case find isLocation (rspHeaders resp) of
                        Nothing            -> throwIO $ FailedRedirection resp
                        Just (Header _ q') -> do
                            putStrLn $ "Warning: Redirected to " ++ q'
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
reify = reifyHTTPErrors . reifyStreamErrors

-- | Like reify, but returns Left for Redirect errors, instead of throwing them.
reifyRed :: IO (Result (Response String)) -> IO (Either (Response String) String)
reifyRed = reifyHTTPErrorsRed . reifyStreamErrors

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
        _       -> throwIO $ HttpError $ resp

reifyHTTPErrors :: IO (Response String) -> IO String
reifyHTTPErrors action = do
    resp <- reifyHTTPErrorsRed action
    case resp of
        Left resp -> throwIO $ HttpRedirect resp
        Right str -> return str

-- Another possible exception is IOError {ioe_type == NoSuchThing}
-- Should we wrap also this?
data RQliteError =
      UnexpectedResponse String
    | StreamError ConnError
    | HttpError (Response String) -- Does the user really need the whole response here?
    | HttpRedirect (Response String)
                     -- since RQlite is a distributed db, redirections to the leader
                     -- deserve a different constructor, even though technically it
                     -- is just another http error code.
                     -- We may need to provide options to the user and make the redirection
                     -- ourselves.
    | MaxNumberOfRedirections [Response String]
    | FailedRedirection (Response String)
    deriving (Show, Typeable, Exception)
