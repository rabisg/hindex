{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GADTs            #-}
module HIndex.Types where

import           HIndex.Serializable

import           Control.Concurrent.MVar
import qualified Data.ByteString.Lazy    as LB
import qualified Data.HashTable.IO       as HT
import           Data.Map
import qualified Data.Text               as T
import           Data.Trie               (Trie)
import           GHC.Int                 (Int64)


type Offset = Int64

type TermIndex = Trie Offset

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
                         , segmentTermsBS :: [LB.ByteString]
                         , segmentIndex   :: TermIndex
                         }

data HIndexConfig  = HIndexConfig { hBaseDirectory :: FilePath }

type HIndexValue a = (Serializable a, Ord a)
data HIndex a where
  HIndex :: (HIndexValue a) =>
            { hConfig         :: HIndexConfig
            , hCurSegment     :: MVar (InMemorySegment a)
            , hCurSegmentNum  :: MVar Int
            , hActiveSegments :: MVar (Map Int TermIndex)
            , hDeletedDocs    :: MVar [IndexId a]
            } -> HIndex a
