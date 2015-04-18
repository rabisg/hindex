{-# LANGUAGE TypeFamilies #-}
module Test.Types where

import           HIndex

import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put

data InvertedIndex = InvertedIndex Int
                     deriving (Show, Eq, Ord)

instance Binary InvertedIndex where
  put (InvertedIndex i) = putWord32le $ fromIntegral i
  get = do
    i <- getWord32le
    return $ InvertedIndex (fromIntegral i)

instance HIndexValue InvertedIndex where
  type IndexId InvertedIndex = Int
  indexId (InvertedIndex i) = i
