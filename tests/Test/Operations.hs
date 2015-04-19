module Test.Operations where

import           HIndex
import           Test.Types

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
  ]
  where
    init_ :: IO (HIndex Int TermFreq)
    init_ = do
      tmpDir <- getTemporaryDirectory
      let conf = HIndexConfig $ tmpDir </> "hindex_test"
      initIndex conf

    -- | Remove test files
    destroy index = removeDirectoryRecursive . hBaseDirectory . getConfig $ index
