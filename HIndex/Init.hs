{-# LANGUAGE ConstraintKinds #-}
module HIndex.Init (initIndex) where

import           HIndex.InMemorySegment
import           HIndex.Types

import           Control.Concurrent.MVar
import           Data.Map
import           System.Directory

getNextSegmentNum :: IO Int
getNextSegmentNum = return 1

getActiveSegments :: IO (Map Int TermFST)
getActiveSegments = return empty

initIndex :: (HIndexValue a) => HIndexConfig -> IO (HIndex a)
initIndex config = do
  createDirectoryIfMissing True (hBaseDirectory config)
  seg <-  newInMemorySegment
  mSeg <- newMVar seg
  n <-  getNextSegmentNum
  mSegN <- newMVar n
  activeSegments <- getActiveSegments
  mActiveSegments <- newMVar activeSegments
  return HIndex { hConfig = config
                , hCurSegment = mSeg
                , hCurSegmentNum = mSegN
                , hActiveSegments = mActiveSegments
                }
