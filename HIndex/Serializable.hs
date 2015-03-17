{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TypeSynonymInstances #-}
module HIndex.Serializable where

import           Data.ByteString       (ByteString)
import           Data.ByteString.Char8 (pack, unpack)

class Serializable a where
  encode :: a -> ByteString
  decode :: ByteString -> Either String a

instance Serializable String where
  encode = pack
  decode = Right . unpack
