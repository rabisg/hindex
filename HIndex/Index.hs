{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
module HIndex.Index where

import           Control.Arrow         (first)
import           Data.Binary           (Binary, get, put)
import           Data.Binary.Get       (runGet)
import           Data.Binary.Put       (runPut)
import           Data.ByteString.Lazy  (hGetContents, hPut)
import           Data.Text.Encoding    (encodeUtf16LE)
import           Data.Trie             (Trie, lookup)
import           Data.Trie.Convenience (fromListR)
import           Prelude               hiding (lookup)
import           System.IO             (Handle)

import           HIndex.Types

class Index index a | index -> a where
  buildIndex :: [(Key, a)] -> index
  getOffset  :: index -> Key -> Maybe a
  writeIndex :: Handle -> index -> IO ()
  readIndex  :: Handle -> IO index


instance (Binary a) => Index (Trie a) a where
  buildIndex = fromListR . map (first encodeUtf16LE)
  getOffset index key = lookup (encodeUtf16LE key) index
  writeIndex h index = hPut h (runPut . put $ index)
  readIndex h = do
    bs <- hGetContents h
    return $ runGet get bs
