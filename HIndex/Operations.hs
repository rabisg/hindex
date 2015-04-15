{-# LANGUAGE ConstraintKinds #-}
module HIndex.Operations ( put
                         , flush
                         , get
                         ) where

import           HIndex.Constants
import           HIndex.FST
import           HIndex.InMemorySegment
import           HIndex.Segment
import           HIndex.Term
import           HIndex.Types

import           Control.Applicative
import           Control.Concurrent.MVar
import           Data.Binary.Get
import qualified Data.ByteString.Lazy    as LB
import           Data.List.Ordered
import qualified Data.Map                as M
import           System.Directory
import           System.IO
import           System.Posix.Temp

put :: (HIndexValue a) => HIndex a -> Key -> [a] -> IO ()
put hindex k vs = withMVar (hCurSegment hindex) (\seg -> addTerm seg k (sort vs))


mkSeg :: (HIndexValue a) =>  HIndex a -> IO (Segment a)
mkSeg hindex = do
  n <- modifyMVar curSegN (\n -> return (n+1, n))
  newSeg <- newInMemorySegment
  memorySeg <- swapMVar m newSeg
  fromInMemorySegment n memorySeg
  where
    m = hCurSegment hindex
    curSegN = hCurSegmentNum hindex

writeSeg :: (HIndexValue a) => Segment a -> FilePath -> IO (FilePath, FilePath)
writeSeg seg dir = do
  (datFilePath, datHandle) <- mkstemp dir
  (hintFilePath, hintHandle) <- mkstemp dir
  writeSegment seg (datHandle, hintHandle)
  hClose datHandle >> hClose hintHandle
  return (datFilePath, hintFilePath)

addActiveSeg :: HIndex a -> Segment a -> IO ()
addActiveSeg hindex seg = modifyMVar_ activeSegments f
  where
    activeSegments = hActiveSegments hindex
    f = return . M.insert (segmentN seg) (segmentFST seg)

-- |Flushes the in-memory index to disk
flush :: (HIndexValue a) => HIndex a -> IO ()
flush hindex = do
  seg <- mkSeg hindex
  (datFilePath, hintFilePath) <- writeSeg seg filePath
  let n = show $ segmentN seg
  renameFile datFilePath (mkFilePath n dataFileExtension)
  renameFile hintFilePath (mkFilePath n hintFileExtension)
  addActiveSeg hindex seg
  where
    filePath = hBaseDirectory . hConfig $ hindex
    mkFilePath name ext = filePath ++ "/" ++ name ++ ext

readVal :: (HIndexValue a) => FilePath -> Int64 -> IO [a]
readVal segmentPath offset = withFile segmentPath ReadMode readVal'
  where
    readVal' :: (HIndexValue a) => Handle -> IO [a]
    readVal' handle = do
      hSeek handle AbsoluteSeek (fromIntegral offset)
      lenBS <- LB.hGet handle word64Len
      let len = runGet getWord64le lenBS
      termsBS <- LB.hGet handle (fromIntegral len)
      let term = runGet getTerm termsBS
      return $ termValues term
      where
        word64Len = 8

getInSeg :: (HIndexValue a) => FilePath -> Key -> (Int, TermFST) -> IO [a]
getInSeg basePath k (n, termFST) =
  case maybeOffset of
   Nothing -> return []
   Just offset -> readVal segmentFile offset
  where
    maybeOffset = getOffset termFST k
    segmentFile = basePath ++ "/" ++ show n ++ dataFileExtension

get :: (HIndexValue a) => HIndex a -> Key -> IO [a]
get hindex key = do
  activeSegments <- readMVar $ hActiveSegments hindex
  curSeg <- readMVar $ hCurSegment hindex
  listOfVals <- mapM (getInSeg filePath key) (M.toList activeSegments)
  inMemorySegVals <- queryTerm curSeg key
  return $ foldl union inMemorySegVals listOfVals
  where
    filePath = hBaseDirectory . hConfig $ hindex
