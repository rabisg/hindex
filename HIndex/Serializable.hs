{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module HIndex.Serializable where

import qualified Data.Binary          as Bin
import           Data.Binary.Get      (runGetOrFail)
import           Data.Binary.Put      (runPut)
import           Data.ByteString.Lazy (ByteString)

encode :: (Bin.Binary a) => a -> ByteString
encode = runPut . Bin.put

decode :: (Bin.Binary a) => ByteString -> Either String a
decode = f . runGetOrFail Bin.get
    where
      f (Left (_, _, str)) = Left str
      f (Right (_, _, a)) = Right a
