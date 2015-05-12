{-|
  Module      : HIndex.Types
  Description : Type definitions
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# OPTIONS_GHC -fno-warn-orphans  #-}

module HIndex.Types where

import           HIndex.Util.BinaryHelper

import           Control.Applicative      ((<$>), (<*>))
import           Data.Binary
import           Data.IORef
import           Data.Map.Strict
import qualified Data.Text                as T
import qualified Data.Text.Encoding       as E
import           Data.Trie                (Trie)
import           Data.UUID                (UUID)
import           GHC.Int                  (Int64)

-- | Offset (bytes) into the file
type Offset = Int64

-- | Mapping from 'Term' to 'Offset'
-- This implementation determines the kind of operations that the
-- index can support. For example, basic HashMap cannot support distance
-- queries. Current implementation uses 'Data.Trie' which is Patricia
-- tree, first on the characters and then on their bit representation
newtype Dictionary doc p = Dictionary { unDict :: Trie Offset }

-- | Wrapper around 'Data.Text'
-- Denotes the key for all operations on this index
newtype Term doc = Term { unKey :: T.Text }
                 deriving (Show, Eq, Ord)

instance Binary (Term doc) where
    put = put . E.encodeUtf8 . unKey
    get = Term <$> E.decodeUtf8 <$> get


-- | Stores the associated information for a 'Term'
-- Consists of 'DocumentId' and other associated data
-- The associated data is represented by a parametrized type /p/
-- /p/ can be term frequency, relevance score, character offset
-- or a sum type representing all of the above
data Posting doc p where
  Posting :: Document doc p
          => { postingDocId :: DocumentId doc -- ^ unique document id
             , posting      :: p              -- ^ associated data corresponding to this term
             } -> Posting doc p

deriving instance (Show (DocumentId doc), Show b) => Show (Posting doc b)

instance Document doc b => Binary (Posting doc b) where
  put (Posting docId b) = put docId >> put b
  get = Posting <$> get <*> get


-- | __IIEntry__ stands for Inverted Index Entry and is the basic unit of an inverted index.
-- It is a mapping from term to postings list where additionally
-- posting(s) are stored in sorted order for easier manipulation
data IIEntry doc p where
  IIEntry :: { term    :: Term doc              -- ^ key (string) for this term
             , postingsList :: [Posting doc p] -- ^ List of posting corresponding to this key
             } -> IIEntry doc p

deriving instance (Show (DocumentId doc), Show p) => Show (IIEntry doc p)

instance (Document doc p) => Binary (IIEntry doc p) where
  put IIEntry{..} = put term >> putListOf put postingsList
  get = IIEntry <$> get <*> getListOf get


-- | Data structure for holding the 'TermValue's in memory until
-- they are written to disk by flush operation
type InMemorySegment doc p = Trie [Posting doc p]


-- | Configuration parameters
data HIndexConfig  = HIndexConfig { hBaseDirectory :: FilePath  -- ^ Base directory to store files
                                  }

-- | Alias for segment id
type SegmentId = UUID

-- | Index Handle
data HIndex doc p where
  HIndex :: Document doc p =>
            { -- | Current configuration of the running index
              hConfig         :: HIndexConfig
              -- | The in-memory terms of the index
            , hCurSegment     :: IORef (InMemorySegment doc b)
              -- | Next segment number to write to
            , hCurSegmentNum  :: IORef SegmentId
              -- | The term indices of the active segments
              -- Currently the term indices of all active segments are stored
              -- in memory. This provides a memory limitation to the implementation.
            , hActiveSegments :: IORef (Map SegmentId (Dictionary doc p))
              -- | List of deleted document ids. Stored in memory and written
              -- to disk with each flush operation
            , hDeletedDocs    :: IORef [DocumentId doc]
            } -> HIndex a p


-- | Typeclass to capture data types that can be represented as documents
class ( Ord (DocumentId doc), Binary (DocumentId doc),
        Binary p
      ) => Document doc p where
  -- | Associated data type to denote the /id/ of documents
  -- It is the responsibility of the user to ensure that the /id/
  -- of the documents are unique
  data DocumentId doc
  -- | Processes a document to retrieve the terms
  -- Returns a list of tuples of /Key/ and /b/ where /b/ can be any
  -- arbitrary information corresponding to the term
  -- Together with /DocumentId/ this is used to construct the list of 'Term'
  getTerms :: doc -> [(Term doc, p)]
