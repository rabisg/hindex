{-# LANGUAGE ConstraintKinds #-}
module HIndex.Operations ( delete
                         , put
                         , flush
                         , get
                         ) where

import           HIndex.Constants
import           HIndex.Index
import           HIndex.InMemorySegment
import           HIndex.Segment
import           HIndex.Term
import           HIndex.Types
import           HIndex.Util.BinaryHelper

import           Control.Concurrent.MVar
import           Data.Binary              (Binary)
import qualified Data.Binary              as Bin (put)
import           Data.Binary.Get
import           Data.Binary.Put          (runPut)
import qualified Data.ByteString.Lazy     as LB
import           Data.List.Ordered
import qualified Data.Map                 as M
import           GHC.Int                  (Int64)
import           System.Directory
import           System.FilePath.Posix    ((<.>), (</>))
import           System.IO
import           System.Posix.Temp

put :: HIndex a b -> HIndexDocument a b -> IO ()
put hindex (HIndexDocument docId fields) = withMVar (hCurSegment hindex) f
  where f seg = mapM_ (\(k, vs) -> addTerm seg k (TermValue docId vs)) fields


mkSeg :: (HIndexDocId a, HIndexValue b) => HIndex a b -> IO (Segment a b)
mkSeg hindex = do
  n <- modifyMVar curSegN (\n -> return (n+1, n))
  newSeg <- newInMemorySegment
  memorySeg <- swapMVar m newSeg
  fromInMemorySegment n memorySeg
  where
    m = hCurSegment hindex
    curSegN = hCurSegmentNum hindex

writeSeg :: (HIndexValue a) => Segment a b -> FilePath -> IO (FilePath, FilePath)
writeSeg seg dir = do
  (datFilePath, datHandle) <- mkstemp dir
  (hintFilePath, hintHandle) <- mkstemp dir
  writeSegment seg (datHandle, hintHandle)
  hClose datHandle >> hClose hintHandle
  return (datFilePath, hintFilePath)

writeDeleted :: Binary a => [a] -> FilePath -> IO FilePath
writeDeleted delDocs dir = do
  (delFilePath, delHandle) <- mkstemp dir
  LB.hPut delHandle $ runPut (putListOf Bin.put delDocs)
  return delFilePath

addActiveSeg :: HIndex a b -> Segment a b -> IO ()
addActiveSeg hindex seg = modifyMVar_ activeSegments f
  where
    activeSegments = hActiveSegments hindex
    f = return . M.insert (segmentN seg) (segmentIndex seg)

-- |Flushes the in-memory index to disk
flush :: (HIndexDocId a, HIndexValue b) => HIndex a b -> IO ()
flush hindex = do
  seg <- mkSeg hindex
  deletedDocs <- readMVar (hDeletedDocs hindex)
  (datFilePath, hintFilePath) <- writeSeg seg filePath
  delFilePath <- writeDeleted deletedDocs filePath
  let n = show $ segmentN seg
  renameFile datFilePath (mkFilePath n dataFileExtension)
  renameFile hintFilePath (mkFilePath n hintFileExtension)
  renameFile delFilePath deletedDocsFileName
  addActiveSeg hindex seg
  where
    filePath = hBaseDirectory . hConfig $ hindex
    mkFilePath name ext = filePath ++ "/" ++ name ++ ext

readVal :: (HIndexDocId a, HIndexValue b) => FilePath -> Int64 -> IO [TermValue a b]
readVal segmentPath offset = withFile segmentPath ReadMode readVal'
  where
    readVal' :: (HIndexDocId a, HIndexValue b) => Handle -> IO [TermValue a b]
    readVal' handle = do
      hSeek handle AbsoluteSeek (fromIntegral offset)
      lenBS <- LB.hGet handle word64Len
      let len = runGet getWord64le lenBS
      termsBS <- LB.hGet handle (fromIntegral len)
      let term = runGet getTerm termsBS
      return $ termValues term
      where
        word64Len = 8

getInSeg :: (HIndexDocId a, HIndexValue b) => FilePath -> Key -> (Int, TermIndex) -> IO [TermValue a b]
getInSeg basePath k (n, termFST) =
  case maybeOffset of
   Nothing -> return []
   Just offset -> readVal segmentFile offset
  where
    maybeOffset = getOffset termFST k
    segmentFile = basePath ++ "/" ++ show n ++ dataFileExtension

get :: (HIndexDocId a, HIndexValue b) => HIndex a b -> Key -> IO [TermValue a b]
get hindex key = do
  activeSegments <- readMVar $ hActiveSegments hindex
  curSeg <- readMVar $ hCurSegment hindex
  listOfVals <- mapM (getInSeg filePath key) (M.toList activeSegments)
  inMemorySegVals <- queryTerm curSeg key
  return $ foldl union inMemorySegVals listOfVals
  where
    filePath = hBaseDirectory . hConfig $ hindex

delete :: (HIndexDocId a) => HIndex a b -> a -> IO ()
delete hindex docId = modifyMVar_ (hDeletedDocs hindex) (return . insertSet docId)
