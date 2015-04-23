module HIndex.Util.BinaryHelper where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Lazy as LB

type Putter a = a -> PutM ()

putByteString' :: LB.ByteString -> Put
putByteString' b =  do
  putWord64le $ (fromIntegral . LB.length) b
  putLazyByteString b

putListOf :: Putter a -> Putter [a]
putListOf pa = go 0 (return ())
  where
  go n body []     = putWord64le n >> body
  go n body (x:xs) = n' `seq` go n' (body >> pa x) xs
    where n' = n + 1


getByteString' :: Get LB.ByteString
getByteString' = do
  size <- getWord64le
  getLazyByteString (fromIntegral size)

getListOf :: Get a -> Get [a]
getListOf m = go [] =<< getWord64le
  where
  go as 0 = return (reverse as)
  go as i = do x <- m
               x `seq` go (x:as) (i - 1)
