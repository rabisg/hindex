{-# LANGUAGE ConstraintKinds #-}
module HIndex.Segment ( writeSegment
                      , fromInMemorySegment
                      ) where

import           HIndex.FST
import           HIndex.Term
import           HIndex.Types

import qualified Data.ByteString    as B
import qualified Data.HashTable.IO  as HT
import           Data.List
import           Data.Serialize.Put
import           System.IO

writeSegment :: (HIndexValue a) => Segment a -> (Handle, Handle) -> IO ()
writeSegment seg (datHandle, _) = B.hPut datHandle bs
  where bs = runPut len `B.append` termsBS
        len = putWord64le $ fromIntegral (B.length termsBS)
        termsBS = B.concat $ segmentTermsBS seg

fromInMemorySegment :: (HIndexValue a) => Int -> InMemorySegment a -> IO (Segment a)
fromInMemorySegment n curSeg = do
  xs <- HT.toList curSeg
  return . buildSegment n . sort $ map toTerm xs
  where
    toTerm :: (Key, [a]) -> Term a
    toTerm (k, v) = Term k v

buildSegment ::  (HIndexValue a) => Int -> [Term a] -> Segment a
buildSegment n terms = Segment { segmentN = n
                               , segmentTerms = terms
                               , segmentTermsBS = termsBS
                               , segmentFST = termFST
                               }
  where
    termsBS = map (runPut . putTerm) terms
    ts = scanl accum headerLength termsBS
    accum len term = len + B.length term + word64Len
    word64Len = 8
    headerLength = 0
    termFST = buildFST $ zip termKeys ts
    termKeys = map termKey terms
