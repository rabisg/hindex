{-# LANGUAGE ConstraintKinds #-}
module HIndex.Operations ( delete
                         , put
                         , flush
                         , get
                         ) where

import           HIndex.Constants
import           HIndex.Index
import           HIndex.InMemorySegment
import           HIndex.Metadata
import           HIndex.Segment
import           HIndex.Term
import           HIndex.Types
import           HIndex.Util.BinaryHelper

import           Control.Concurrent.MVar
import           Data.Binary              (Binary)
import qualified Data.Binary              as Bin (put)
import           Data.Binary.Put          (runPut)
import qualified Data.ByteString.Lazy     as LB
import           Data.List.Ordered
import qualified Data.Map.Strict          as M
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

-- | Should be called after mkSeg
-- TODO: Find a way to do $mkSeg$ and $mkMetadata$ atomically
mkMetadata :: HIndex a b -> IO Metadata
mkMetadata hindex = do
  activeSegs <- readMVar $ hActiveSegments hindex
  segN <- readMVar $ hCurSegmentNum hindex
  return Metadata { metaNextSegN = segN
                  , metaActiveSegs = M.keys activeSegs
                  }

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
  hClose delHandle
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
  (datFilePath, hintFilePath) <- writeSeg seg baseDir
  delFilePath <- writeDeleted deletedDocs baseDir
  let n = show $ segmentN seg
  renameFile datFilePath (mkFilePath n dataFileExtension)
  renameFile hintFilePath (mkFilePath n hintFileExtension)
  renameFile delFilePath $ baseDir </> deletedDocsFileName
  addActiveSeg hindex seg
  -- | Write the metadata
  metadata <- mkMetadata hindex
  writeMetadata config metadata
  where
    config = hConfig hindex
    baseDir = hBaseDirectory config
    mkFilePath name ext = baseDir </> name <.> ext

readVal :: (HIndexDocId a, HIndexValue b) => FilePath -> Int64 -> IO [TermValue a b]
readVal segmentPath offset = withFile segmentPath ReadMode readVal'
  where
    readVal' :: (HIndexDocId a, HIndexValue b) => Handle -> IO [TermValue a b]
    readVal' handle = do
      hSeek handle AbsoluteSeek (fromIntegral offset)
      term <- readTerm handle
      return $ termValues term

getInSeg :: (HIndexDocId a, HIndexValue b) => FilePath -> Key ->
            Int -> TermIndex -> IO [TermValue a b] -> IO [TermValue a b]
getInSeg basePath key segNum termIndex res = do
  xs <- res
  xs' <- case maybeOffset of
    Nothing -> return []
    Just offset -> readVal segmentFile offset
  return $ xs ++ xs'
  where
    maybeOffset = getOffset termIndex key
    segmentFile = basePath </> show segNum <.> dataFileExtension

get :: (HIndexDocId a, HIndexValue b) => HIndex a b -> Key -> IO [TermValue a b]
get hindex key = do
  activeSegments <- readMVar $ hActiveSegments hindex
  curSeg <- readMVar $ hCurSegment hindex
  let inMemorySegVals = queryTerm curSeg key
  vals <- M.foldrWithKey' (getInSeg filePath key) inMemorySegVals activeSegments
  deletedDocIds <- readMVar $ hDeletedDocs hindex
  let deletedDocs = map (`TermValue` undefined) deletedDocIds
  return $ vals `minus` deletedDocs
  where
    filePath = hBaseDirectory . hConfig $ hindex

delete :: (HIndexDocId a) => HIndex a b -> a -> IO ()
delete hindex docId = modifyMVar_ (hDeletedDocs hindex) (return . insertSet docId)
