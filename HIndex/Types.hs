{-# LANGUAGE ConstraintKinds #-}
module HIndex.Types where

import           HIndex.Serializable

import           Control.Concurrent.MVar
import qualified Data.ByteString         as B
import qualified Data.HashTable.IO       as HT
import           Data.Map
import qualified Data.Text               as T

-- |Placeholder. To be replaced by a concrete implementation of FST
data TermFST = TermFST (Map Key Int)

type Key = T.Text

data Term a = Term { termKey    :: Key
                   , termValues :: [a]
                   }

instance Eq (Term a) where
  lhs == rhs = termKey lhs == termKey rhs

instance Ord (Term a) where
  lhs <= rhs = termKey lhs <= termKey rhs

type InMemorySegment a = HT.BasicHashTable Key [a]

data Segment a = Segment { segmentN       :: Int
                         , segmentTerms   :: [Term a]
                         , segmentTermsBS :: [B.ByteString]
                         , segmentFST     :: TermFST
                         }

data HIndexConfig  = HIndexConfig { hBaseDirectory :: FilePath }

data HIndex a = HIndex { hConfig         :: HIndexConfig
                       , hCurSegment     :: MVar (InMemorySegment a)
                       , hCurSegmentNum  :: MVar Int
                       , hActiveSegments :: MVar (Map Int TermFST)
                       }

type HIndexValue a = (Serializable a, Ord a)
