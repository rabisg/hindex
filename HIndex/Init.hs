{-# LANGUAGE ConstraintKinds #-}
module HIndex.Init (initIndex) where

import           HIndex.Constants
import           HIndex.InMemorySegment
import           HIndex.Metadata
import           HIndex.Types
import           HIndex.Util.BinaryHelper
import           HIndex.Util.IOHelper

import           Control.Concurrent.MVar
import           Data.Binary              (Binary, get)
import           Data.Binary.Get          (runGet)
import qualified Data.ByteString.Lazy     as LB
import           Data.Map.Strict          (Map, fromList)
import           System.Directory
import           System.FilePath.Posix    ((<.>), (</>))
import           System.IO                (IOMode (..), withFile)

getActiveSegments :: FilePath -> Metadata -> IO (Map Int TermIndex)
getActiveSegments baseDir metadata = do
  activeSegs <- mapM readIndex activeSegsN
  return $ fromList activeSegs
  where
    readIndex :: Int -> IO (Int, TermIndex)
    readIndex n = withFile (mkFilePath n) ReadMode $ \handle -> do
      bs <- LB.hGetContents handle
      case runGetIncremental get `pushChunks` bs of
       Done _ _ termIndex -> return (n, termIndex)
       _ -> fail $ "Could not read index for segment " ++ show n
    activeSegsN = metaActiveSegs metadata
    mkFilePath n = baseDir </> show n <.> hintFileExtension

getDeletedDocs :: (Binary a) => IO [a]
getDeletedDocs = withFileIfExists deletedDocsFileName [] ReadMode getDeletedDocsFromHandle
  where
    getDeletedDocsFromHandle h = do
      bs <- LB.hGetContents h
      return $ runGet (getListOf get) bs

initIndex :: (HIndexDocId a, HIndexValue b) => HIndexConfig -> IO (HIndex a b)
initIndex config = do
  createDirectoryIfMissing True baseDir
  mSeg <- newInMemorySegment >>= newMVar
  metadata <- readMetadata config
  mSegN <- newMVar $ metaNextSegN metadata
  mActiveSegments <- getActiveSegments baseDir metadata >>= newMVar
  mDeletedDocs <- getDeletedDocs >>= newMVar
  return HIndex { hConfig = config
                , hCurSegment = mSeg
                , hCurSegmentNum = mSegN
                , hActiveSegments = mActiveSegments
                , hDeletedDocs = mDeletedDocs
                }
  where
    baseDir = hBaseDirectory config
