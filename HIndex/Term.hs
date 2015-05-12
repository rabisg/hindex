{-# LANGUAGE ConstraintKinds #-}
module HIndex.Term ( readTerm
                   , readTermMaybe
                   ) where

import           HIndex.Types

import           Control.Applicative  ((<$>))
import           Data.Binary          (get)
import           Data.Binary.Get      (getWord64le, runGet)
import qualified Data.ByteString.Lazy as LB
import           Data.Maybe           (fromJust)
import           System.IO            (Handle)

readTerm :: (HIndexDocId a, HIndexValue b) => Handle -> IO (Term a b)
readTerm handle = fromJust <$> readTermMaybe handle

readTermMaybe :: (HIndexDocId a, HIndexValue b) => Handle -> IO (Maybe (Term a b))
readTermMaybe handle = do
  lenBS <- LB.hGet handle word64Len
  if LB.null lenBS then
    return Nothing
    else let len = runGet getWord64le lenBS
         in do
           termsBS <- LB.hGet handle (fromIntegral len)
           return $ Just $ runGet get termsBS
  where
    word64Len = 8
