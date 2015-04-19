{-# LANGUAGE OverloadedStrings #-}
module Test.Operations where

import           HIndex
import           Test.Types

import           Data.List.Ordered (sort)
import           System.Directory
import           System.FilePath
import           Test.Tasty
import           Test.Tasty.HUnit

tests :: TestTree
tests = withResource init_ destroy $ \ioIndex ->
  testGroup "Basic Operations"
  [ testCase "directory should exist" $ do
       index <- ioIndex
       dirExists <- doesDirectoryExist $ hBaseDirectory . getConfig $ index
       assertBool "Base Directory does not exist" dirExists

  , testCase "put should succeed" $ do
       index <- ioIndex
       put index $ HIndexDocument documentId [ ("foo", TermFreq freq)
                                             , ("bar", TermFreq freq')
                                             ]
       put index $ HIndexDocument documentId' [ ("baz", TermFreq freq'')
                                              , ("bar", TermFreq freq''')
                                              ]

  , testCase "get should return the value" $ do
       index <- ioIndex
       vs <- get index "foo"
       assertEqual "get should return same number of results" (length vs) 1
       let (TermValue docId termFreq) = head vs
       assertEqual "docId should be same" docId documentId
       assertEqual "docId should be same" termFreq (TermFreq freq)

  , testCase "multi-value get" $ do
       index <- ioIndex
       vs <- get index "bar"
       assertEqual "get <foo> should return aggregate results" (length vs) 2
       assertEqual "results should be sorted" (sort vs) vs

  , testCase "flush should write files to disk" $ do
       index <- ioIndex
       flush index
       let baseDir = hBaseDirectory . getConfig $ index
       assertFileExists "segment file should exist" $ baseDir </> "1.dat"
       assertFileExists "hint file should exist" $ baseDir </> "1.hint"
  ]
  where
    init_ :: IO (HIndex Int TermFreq)
    init_ = do
      tmpDir <- getTemporaryDirectory
      let conf = HIndexConfig $ tmpDir </> "hindex_test"
      initIndex conf

    -- | Remove test files
    destroy index = removeDirectoryRecursive . hBaseDirectory . getConfig $ index

    -- | Helper method to assert $fp$ exists
    assertFileExists msg fp = do
      fileExists <- doesFileExist fp
      assertBool msg fileExists

    -- | Constants
    documentId = 1
    documentId' = 2
    freq = 5
    freq' = 10
    freq'' = 15
    freq''' = 10
