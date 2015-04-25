{-# LANGUAGE OverloadedStrings #-}
module Test.Operations ( tests
                       , testsOnExistingIndex
                       ) where

import           HIndex
import           HIndex.Internal
import           Test.Types

import           Control.Concurrent.MVar
import           Data.List.Ordered       (sort)
import qualified Data.Map.Strict         as M
import           System.Directory
import           System.FilePath
import           Test.Tasty
import           Test.Tasty.HUnit

-- | Constants
documentId, documentId' :: Int
documentId = 1
documentId' = 2

fooFreq, barFreq, bazFreq, barFreq':: Int
fooFreq = 5
barFreq = 10
bazFreq = 15
barFreq' = 1

-- | Get the HIndex resource
init_ :: IO (HIndex Int TermFreq)
init_ = do
  tmpDir <- getTemporaryDirectory
  let conf = HIndexConfig $ tmpDir </> "hindex_test"
  initIndex conf

-- | Remove test files
destroy :: HIndex a b -> IO ()
destroy index = removeDirectoryRecursive . hBaseDirectory . getConfig $ index

getFooTest :: IO (HIndex Int TermFreq) -> String -> TestTree
getFooTest ioIndex msg = testCase msg $ do
  index <- ioIndex
  vs <- get index "foo"
  assertEqual "get should return same number of results" (length vs) 1
  let (TermValue docId termFreq) = head vs
  assertEqual "docId should be same" docId documentId
  assertEqual "docId should be same" termFreq (TermFreq fooFreq)

delDocTest :: IO (HIndex Int TermFreq) -> String -> TestTree
delDocTest ioIndex msg = testCase msg $ do
  index <- ioIndex
  getBaz <- get index "baz"
  getBar <- get index "bar"
  assertBool "baz should have no results" (null getBaz)
  assertBool "bar should not have the deleted document" (notMember getBar documentId')
  where
    notMember [] _ =  True
    notMember (x:xs) deletedDocId = case x of
      TermValue docId _ | docId == deletedDocId -> False
                        | otherwise -> notMember xs docId

tests :: TestTree
tests = withResource init_ nothing $ \ioIndex ->
  testGroup "Basic Operations"
  [ testCase "directory should exist" $ do
       index <- ioIndex
       dirExists <- doesDirectoryExist $ hBaseDirectory . getConfig $ index
       assertBool "Base Directory does not exist" dirExists

  , testCase "put should succeed" $ do
       index <- ioIndex
       put index $ HIndexDocument documentId [ ("foo", TermFreq fooFreq)
                                             , ("bar", TermFreq barFreq)
                                             ]
       put index $ HIndexDocument documentId' [ ("baz", TermFreq bazFreq)
                                              , ("bar", TermFreq barFreq')
                                              ]

  , getFooTest ioIndex "get should return the same value"

  , testCase "multi-value get" $ do
       index <- ioIndex
       vs <- get index "bar"
       assertEqual "get <bar> should return aggregate results" (length vs) 2
       assertEqual "results should be sorted" (sort vs) vs

  , testCase "delete should succeed" $ do
       index <- ioIndex
       delete index documentId'

  , delDocTest ioIndex "deleted results should not be present when in memory"

  , testCase "flush should write files to disk" $ do
       index <- ioIndex
       flush index
       let baseDir = hBaseDirectory . getConfig $ index
       assertFileExists "segment file should exist" $ baseDir </> "1.dat"
       assertFileExists "hint file should exist" $ baseDir </> "1.hint"

  , getFooTest ioIndex "get should return the values after flush"

  , delDocTest ioIndex "Deleted documents should not be present after flush"
  ]
  where
    nothing = const $ return ()
    -- | Helper method to assert $fp$ exists
    assertFileExists msg fp = do
      fileExists <- doesFileExist fp
      assertBool msg fileExists

testsOnExistingIndex :: TestTree
testsOnExistingIndex = withResource init_ destroy $ \ioIndex ->
  testGroup "Operations on previously saved index"
  [ testCase "Previously stored state should be retrieved" $ do
       index <- ioIndex
       activeSegs <- readMVar $ hActiveSegments index
       deletedDocIds <- readMVar $ hDeletedDocs index
       assertBool "Segments should be retrieved" (not . M.null $ activeSegs)
       assertBool "Deleted documents should be retrieved" (not . null $ deletedDocIds)

  , getFooTest ioIndex "get should return the previously stored values"

  , delDocTest ioIndex "Deleted documents should not be present"
  ]
