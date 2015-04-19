{-# LANGUAGE ConstraintKinds #-}
module HIndex.InMemorySegment ( addTerm
                              , queryTerm
                              , newInMemorySegment
                              ) where

import           HIndex.Types

import           Control.Applicative
import           Data.HashTable.IO   as HT
import           Data.List.Ordered

queryTerm :: InMemorySegment a b -> Key -> IO [TermValue a b]
queryTerm seg k = f <$> HT.lookup seg k
  where
    f Nothing = []
    f (Just xs) = xs

addTerm' ::  (TermValue a b -> TermValue a b -> Ordering)
             -> InMemorySegment a b -> Key -> TermValue a b -> IO ()
addTerm' ord seg k vs = do
  maybeValue <- HT.lookup seg k
  let vs'' = case maybeValue of
              Nothing ->  [vs]
              Just vs' -> insertSetBy ord vs vs'
    in  HT.insert seg k vs''

addTerm :: (HIndexDocId a) => InMemorySegment a b -> Key -> TermValue a b -> IO ()
addTerm = addTerm' compare

newInMemorySegment :: IO (InMemorySegment a b)
newInMemorySegment = HT.new
