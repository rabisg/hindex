{-# LANGUAGE ConstraintKinds  #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}
{-# LANGUAGE GADTs            #-}
module HIndex.Types where

import           Control.Concurrent.MVar
import           Data.Binary
import qualified Data.ByteString.Lazy    as LB
import qualified Data.HashTable.IO       as HT
import           Data.Map
import qualified Data.Text               as T
import           Data.Trie               (Trie)
import           GHC.Int                 (Int64)


type Offset = Int64

type TermIndex = Trie Offset

type Key = T.Text

data TermValue a b where
  TermValue :: (HIndexDocId a, HIndexValue b) =>
               a -> b -> TermValue a b

instance (Show a, Show b) => Show (TermValue a b) where
  show (TermValue a b) = "Id = " ++ show a ++ " Value = " ++ show b

instance Eq (TermValue a b) where
  (TermValue a _) == (TermValue a' _) = a == a'

instance Ord (TermValue a b) where
  (TermValue a _) <= (TermValue a' _) = a <= a'

instance (HIndexDocId a, HIndexValue b) => Binary (TermValue a b) where
  put (TermValue a b) = put a >> put b
  get = do
    a <- get
    b <- get
    return $ TermValue a b


data Term a b where
  Term ::  { termKey    :: Key
          , termValues :: [TermValue a b]
          } -> Term a b

instance Eq (Term a b) where
  lhs == rhs = termKey lhs == termKey rhs

instance Ord (Term a b) where
  lhs <= rhs = termKey lhs <= termKey rhs


type InMemorySegment a b = HT.BasicHashTable Key [TermValue a b]

data Segment a b = Segment { segmentN       :: Int
                           , segmentTerms   :: [Term a b]
                           , segmentTermsBS :: [LB.ByteString]
                           , segmentIndex   :: TermIndex
                           }

data HIndexConfig  = HIndexConfig { hBaseDirectory :: FilePath }

data HIndex a b where
  HIndex :: (HIndexValue b) =>
            { hConfig         :: HIndexConfig
            , hCurSegment     :: MVar (InMemorySegment a b)
            , hCurSegmentNum  :: MVar Int
            , hActiveSegments :: MVar (Map Int TermIndex)
            , hDeletedDocs    :: MVar [a]
            } -> HIndex a b

type HIndexValue a = (Binary a)

type HIndexDocId a = (Ord a, Binary a)

data HIndexDocument a b where
  HIndexDocument :: (HIndexDocId a, HIndexValue b) => a -> [(Key, b)] -> HIndexDocument a b
