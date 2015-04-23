{-# LANGUAGE ConstraintKinds #-}
module HIndex.Segment ( writeSegment
                      , fromInMemorySegment
                      ) where

import           HIndex.Index
import           HIndex.Term
import           HIndex.Types
import           HIndex.Util.BinaryHelper

import           Data.Binary.Put
import qualified Data.ByteString.Lazy     as LB
import qualified Data.HashTable.IO        as HT
import           Data.List
import           System.IO

writeSegment :: Segment a b -> (Handle, Handle) -> IO ()
writeSegment seg (datHandle, hintHandle) = do
  writeIndex hintHandle (segmentIndex seg)
  LB.hPut datHandle (LB.concat bs)
  where
    bs = map (runPut . putByteString') (segmentTermsBS seg)

fromInMemorySegment :: (HIndexDocId a, HIndexValue b)
                       => Int -> InMemorySegment a b -> IO (Segment a b)
fromInMemorySegment n curSeg = do
  xs <- HT.toList curSeg
  return . buildSegment n . sort $ map toTerm xs
  where
    toTerm (k, v) = Term k v

buildSegment :: (HIndexDocId a, HIndexValue b)
                => Int -> [Term a b] -> Segment a b
buildSegment n terms = Segment { segmentN = n
                               , segmentTerms = terms
                               , segmentTermsBS = termsBS
                               , segmentIndex = termIndex
                               }
  where
    termsBS = map (runPut . putTerm) terms
    ts = scanl accum headerLength termsBS
    accum len term = len + LB.length term + word64Len
    word64Len = 8
    headerLength = 0
    termIndex = buildIndex $ zip termKeys ts
    termKeys = map termKey terms
