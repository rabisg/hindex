{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE StandaloneDeriving   #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module HIndex.Types where

import           HIndex.Util.BinaryHelper

import           Control.Applicative     ((<$>))
import           Control.Concurrent.MVar
import           Data.Binary
import qualified Data.ByteString.Lazy    as LB
import qualified Data.HashTable.IO       as HT
import           Data.Map.Strict
import qualified Data.Text               as T
import           Data.Trie               (Trie)
import           GHC.Int                 (Int64)
import qualified Data.Text.Encoding      as E

type Offset = Int64

type TermIndex = Trie Offset

type Key = T.Text

instance Binary Key where
    put = put . E.encodeUtf8
    get = E.decodeUtf8 <$> get

data TermValue a b where
  TermValue :: (HIndexDocId a, HIndexValue b) =>
               a -> b -> TermValue a b

deriving instance (Show a, Show b) => Show (TermValue a b)

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

deriving instance (Show a, Show b) => Show (Term a b)

instance (HIndexDocId a, HIndexValue b) => Binary (Term a b) where
  put Term{..} = put termKey >> putListOf put termValues
  get = do
    key <- get
    values <- getListOf get
    return $ Term key values

instance Eq (Term a b) where
  lhs == rhs = termKey lhs == termKey rhs

instance Ord (Term a b) where
  lhs <= rhs = termKey lhs <= termKey rhs


type InMemorySegment a b = HT.BasicHashTable Key [TermValue a b]

data Segment a b = Segment { segmentN       :: Int
                           , segmentTerms   :: [Term a b]
                           , segmentTermsBS :: [LB.ByteString]
                           , segmentIndex   :: TermIndex
                           } deriving (Show)

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
