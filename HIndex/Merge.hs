{-# LANGUAGE ConstraintKinds #-}
module HIndex.Merge (merge) where

import           HIndex.Constants
import           HIndex.Index
import           HIndex.Term
import           HIndex.Types

import           Control.Concurrent.MVar
import           Data.Binary             (put)
import           Data.Binary.Put         (runPut)
import qualified Data.ByteString.Lazy    as LB
import           Data.List.Ordered       (minus, union)
import qualified Data.Map.Strict         as M
import           Data.Maybe              (fromJust)
import           System.Directory        (removeFile)
import           System.FilePath.Posix   ((<.>), (</>))
import           System.IO

merge :: (HIndexDocId a, HIndexValue b) => HIndex a b -> Int -> Int -> IO ()
merge hindex segA segB = do
  -- Get the next segment number
  segC <- modifyMVar (hCurSegmentNum hindex) (\n -> return (n+1, n))
  -- List of deleted Documents so that removed documents can be garbage collected
  deletedDocIds <- readMVar $ hDeletedDocs hindex
  -- Merge $segA$ & $segB$ into $segC$
  handleA <- openFile segAFilePath ReadMode
  handleB <- openFile segBFilePath ReadMode
  handleC <- openFile (baseDir </> show segC <.> dataFileExtension) WriteMode

  -- get the new offset table and list of documents that were removed
  (offsetTable, removedDocIds) <- mergeFiles hindex deletedDocIds handleA handleB handleC

  -- TODO Find a way to do this atomically
  -- Remove the garbage collected document ids
  modifyMVar_ (hDeletedDocs hindex) $ \xs -> return $ xs `minus` removedDocIds
  -- Update the active Segments map
  modifyMVar_ (hActiveSegments hindex) $ \activeSegs -> do
    let
      -- delete the two merging segments and add the new merged segments
      activeSegs' = M.delete segA activeSegs
      activeSegs'' = M.delete segB activeSegs'
      -- build the index out of offset table
      index = buildIndex offsetTable
    return $ M.insert segC index activeSegs''
  -- Close the handles
  mapM_ hClose [ handleA, handleB, handleC ]
  -- Delete the old segments
  removeFile segAFilePath
  removeFile segBFilePath
  where
    baseDir = hBaseDirectory . hConfig $ hindex
    segAFilePath = baseDir </> show segA <.> dataFileExtension
    segBFilePath = baseDir </> show segB <.> dataFileExtension

mergeFiles ::
  (HIndexDocId a, HIndexValue b)
  => HIndex a b
  -> [a] -> Handle -> Handle -> Handle -> IO ([(Key, Offset)], [a])
mergeFiles hindex' delDocIds' handleA handleB handleC = do
  termA <- readTermMaybe handleA
  termB <- readTermMaybe handleB
  mergeFiles' hindex' delDocIds' termA termB 0
  where
    -- | Removes deleted documents from this term and returns
    -- | the modified term as well as the list of document ids
    -- | that were deleted
    processTerm ::
      (HIndexDocId a, HIndexValue b)
      => [a] -> Term a b -> (Term a b, [a])
    processTerm delDocIds term = (term { termValues = ts `minus` xs }, dels)
      where
        (Term _ ts) = term
        xs = map (`TermValue` undefined) delDocIds
        dels = delDocIds `minus` (map toDocId ts)
        toDocId (TermValue docId _) = docId

    -- | Merges two terms which have the same key
    mergeTerm :: Term a b -> Term a b -> Term a b
    mergeTerm (Term t1Key t1Vals) (Term t2Key t2Vals) =
      if t1Key == t2Key then
        Term t1Key (t1Vals `union` t2Vals)
        else error "Internal Error: mergeTerm should not be called with different keys"

    -- | Selects the smaller of the two values
    -- | Considers $Nothing$ to be greater than any other value so that the case when
    -- | when one file is exhausted is handled correctly
    myComparator :: Maybe (Term a b) -> Maybe (Term a b) -> Ordering
    myComparator (Just t1) (Just t2) = t1 `compare` t2
    myComparator (Just _) Nothing = LT
    myComparator Nothing (Just _) = GT
    myComparator Nothing Nothing =
      error "Internal Error: myComparator should not be called with Nothing Nothing"

    -- | Recursively merges both the segments and writes the output to the
    -- | new segment
    mergeFiles' ::
      (HIndexDocId a, HIndexValue b)
      => HIndex a b -> [a]
      -> Maybe (Term a b) -> Maybe (Term a b) -> Offset -> IO ([(Key, Offset)], [a])
    mergeFiles' _ _ Nothing Nothing _ = return ([], [])
    mergeFiles' hindex delDocIds maybeTermA maybeTermB offset = do
      -- Select the smaller of the two terms
      -- and get the next terms in both segments respectively
      (term, nextTermA, nextTermB) <- case maybeTermA `myComparator` maybeTermB of
        LT -> readTermMaybe handleA >>= (\t -> return (fromJust maybeTermA, t, maybeTermB))
        GT -> readTermMaybe handleB >>= (\t -> return (fromJust maybeTermB, maybeTermA, t))
        EQ -> do
          termANext <- readTermMaybe handleA
          termBNext <- readTermMaybe handleB
          let termA = fromJust maybeTermA
              termB = fromJust maybeTermB
          return (mergeTerm termA termB, termANext, termBNext)
      -- Process the term to remove deleted ids
      -- and get the key with the new offset
      let (processedTerm, delDocsInTerm) = processTerm delDocIds term
          key = termKey processedTerm
          bs = runPut (put processedTerm)
          offset' = offset + LB.length bs
      -- Write the updated term to $segC$
      LB.hPut handleC bs
      -- Recursively call mergeFiles' on the rest of the segment
      (keyOffs, dels) <- mergeFiles' hindex delDocIds nextTermA nextTermB offset'
      -- Tail Recursively add this result to the rest of the results
      return ((key, offset):keyOffs, delDocsInTerm `union` dels)
