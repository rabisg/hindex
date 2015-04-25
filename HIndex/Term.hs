{-# LANGUAGE ConstraintKinds #-}
module HIndex.Term (readTerm) where

import           HIndex.Types

import           Data.Binary          (get)
import           Data.Binary.Get      (getWord64le, runGet)
import qualified Data.ByteString.Lazy as LB
import           System.IO            (Handle)

readTerm :: (HIndexDocId a, HIndexValue b) => Handle -> IO (Term a b)
readTerm handle = do
  lenBS <- LB.hGet handle word64Len
  let len = runGet getWord64le lenBS
  termsBS <- LB.hGet handle (fromIntegral len)
  return $ runGet get termsBS
  where
    word64Len = 8
