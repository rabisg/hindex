{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TypeSynonymInstances #-}
module HIndex.Serializable where

import qualified Data.Binary                as Bin
import           Data.ByteString.Lazy       (ByteString)
import           Data.ByteString.Lazy.Char8 (pack, unpack)
import           GHC.Int                    (Int64)

class Serializable a where
  encode :: a -> ByteString
  decode :: ByteString -> Either String a

instance Serializable String where
  encode = pack
  decode = Right . unpack

instance Serializable Int64 where
  encode = Bin.encode
  decode = Bin.decode
