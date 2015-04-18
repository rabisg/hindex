module HIndex.Init (initIndex) where

import           HIndex.Constants
import           HIndex.InMemorySegment
import           HIndex.Types
import           HIndex.Util.BinaryHelper

import           Control.Concurrent.MVar
import           Data.Binary              (Binary, get)
import           Data.Binary.Get          (runGet)
import qualified Data.ByteString.Lazy     as LB
import           Data.Map
import           System.Directory
import           System.IO

getNextSegmentNum :: IO Int
getNextSegmentNum = return 1

getActiveSegments :: IO (Map Int TermIndex)
getActiveSegments = return empty

getDeletedDocs :: (Binary a) => IO [a]
getDeletedDocs = withFileIfExists deletedDocsFileName ReadMode getDeletedDocsFromHandle
  where
    withFileIfExists filePath ioMode f = do
      exists <- doesFileExist filePath
      if exists then
        withFile filePath ioMode f
        else return []
    getDeletedDocsFromHandle h = do
      bs <- LB.hGetContents h
      return $ runGet (getListOf get) bs

initIndex :: (HIndexValue a) => HIndexConfig -> IO (HIndex a)
initIndex config = do
  createDirectoryIfMissing True (hBaseDirectory config)
  mSeg <- newInMemorySegment >>= newMVar
  mSegN <- getNextSegmentNum >>= newMVar
  mActiveSegments <- getActiveSegments >>= newMVar
  mDeletedDocs <- getDeletedDocs >>= newMVar
  return HIndex { hConfig = config
                , hCurSegment = mSeg
                , hCurSegmentNum = mSegN
                , hActiveSegments = mActiveSegments
                , hDeletedDocs = mDeletedDocs
                }
