{-# LANGUAGE ConstraintKinds #-}
module HIndex.Term ( getTerm
                   , putTerm
                   ) where

import           HIndex.Serializable
import           HIndex.Types

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as LB
import           Data.Either
import qualified Data.Text.Encoding   as E

data PersistentTerm = PTerm { pTermKey    :: B.ByteString
                            , pTermValues :: [LB.ByteString]
                            }


toPersistentTerm :: (HIndexValue a) => Term a -> PersistentTerm
toPersistentTerm term = PTerm { pTermKey = key
                              , pTermValues =  vals
                              }
  where
    key = E.encodeUtf16LE $ termKey term
    vals = map encode $ termValues term

fromPersistentTerm :: (HIndexValue a) => PersistentTerm -> Either String (Term a)
fromPersistentTerm pTerm = if null errors
                           then Right Term { termKey = key
                                           , termValues = vals
                                           }
                           else Left $ head errors
  where
    key = E.decodeUtf16LE $ pTermKey pTerm
    decoded = partitionEithers $ map decode (pTermValues pTerm)
    errors = fst decoded
    vals = snd decoded

type Putter a = a -> PutM ()

putByteString' :: LB.ByteString -> Put
putByteString' b =  do
  putWord64le $ (fromIntegral . LB.length) b
  putLazyByteString b

putListOf :: Putter a -> Putter [a]
putListOf pa = go 0 (return ())
  where
  go n body []     = putWord64be n >> body
  go n body (x:xs) = n' `seq` go n' (body >> pa x) xs
    where n' = n + 1

putPTerm :: PersistentTerm -> Put
putPTerm pTerm = do
  putWord32le $ (fromIntegral . B.length) k
  putByteString k
  putListOf putByteString' v
  where
    k = pTermKey pTerm
    v = pTermValues pTerm

putTerm :: (HIndexValue a) => Term a -> Put
putTerm = putPTerm . toPersistentTerm


getByteString' :: Get LB.ByteString
getByteString' = do
  size <- getWord64le
  getLazyByteString (fromIntegral size)

getListOf :: Get a -> Get [a]
getListOf m = go [] =<< getWord64be
  where
  go as 0 = return (reverse as)
  go as i = do x <- m
               x `seq` go (x:as) (i - 1)

getPTerm :: Get PersistentTerm
getPTerm = do
  keySize <- getWord32le
  key <- getByteString (fromIntegral keySize)
  vals <- getListOf getByteString'
  return $ PTerm key vals

getTerm :: (HIndexValue a) => Get (Term a)
getTerm = do
  pTerm <- getPTerm
  case fromPersistentTerm pTerm of
   Left err -> fail err
   Right term -> return term
