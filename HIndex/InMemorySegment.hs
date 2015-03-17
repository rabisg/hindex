{-# LANGUAGE ConstraintKinds #-}
module HIndex.InMemorySegment ( addTerm
                              , queryTerm
                              , newInMemorySegment
                              ) where

import           HIndex.Types

import           Control.Applicative
import           Data.HashTable.IO   as HT
import           Data.List.Ordered

queryTerm :: InMemorySegment a -> Key -> IO [a]
queryTerm seg k = f <$> HT.lookup seg k
  where
    f Nothing = []
    f (Just xs) = xs

addTerm' ::  (a -> a -> Ordering) -> InMemorySegment a -> Key -> [a] -> IO ()
addTerm' ord seg k vs = do
  maybeValue <- HT.lookup seg k
  let vs'' = case maybeValue of
              Nothing ->  vs
              Just vs' -> unionBy ord vs vs'
    in  HT.insert seg k vs''

addTerm :: (HIndexValue a) => InMemorySegment a -> Key -> [a] -> IO ()
addTerm = addTerm' compare

newInMemorySegment :: IO (InMemorySegment a)
newInMemorySegment = HT.new
