{-# LANGUAGE TypeFamilies #-}
module Test.Types where

import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put

data TermFreq = TermFreq Int
              deriving (Show, Eq, Ord)

instance Binary TermFreq where
  put (TermFreq i) = putWord32le $ fromIntegral i
  get = do
    i <- getWord32le
    return $ TermFreq (fromIntegral i)
