{-# LANGUAGE ConstraintKinds #-}
module HIndex.Segment ( writeSegment
                      , fromInMemorySegment
                      ) where

import           HIndex.Index
import           HIndex.Types

import           Data.Binary          (put)
import           Data.Binary.Put      (putWord64le, runPut)
import qualified Data.ByteString.Lazy as LB
import qualified Data.HashTable.IO    as HT
import           Data.List
import           System.IO

writeSegment :: Segment a b -> (Handle, Handle) -> IO ()
writeSegment seg (datHandle, hintHandle) = do
  writeIndex hintHandle (segmentIndex seg)
  mapM_ (LB.hPut datHandle) bs
  where
    bs = zipWith LB.append len termsBS
    len = map (runPut . putWord64le . fromIntegral . LB.length) termsBS
    termsBS = segmentTermsBS seg

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
    termsBS = map (runPut . put) terms
    ts = scanl accum headerLength termsBS
    accum len term = len + LB.length term + word64Len
    word64Len = 8
    headerLength = 0
    termIndex = buildIndex $ zip termKeys ts
    termKeys = map termKey terms
