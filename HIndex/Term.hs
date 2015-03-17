{-# LANGUAGE ConstraintKinds #-}
module HIndex.Term ( getTerm
                   , putTerm
                   ) where

import           HIndex.Serializable
import           HIndex.Types

import qualified Data.ByteString     as B
import           Data.Either
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Data.Text.Encoding

data PersistentTerm = PTerm { pTermKey    :: B.ByteString
                            , pTermValues :: [B.ByteString]
                            }


toPersistentTerm :: (HIndexValue a) => Term a -> PersistentTerm
toPersistentTerm term = PTerm { pTermKey = key
                              , pTermValues =  vals
                              }
  where
    key = encodeUtf16LE $ termKey term
    vals :: [B.ByteString]
    vals = map encode $ termValues term

fromPersistentTerm :: (HIndexValue a) => PersistentTerm -> Either String (Term a)
fromPersistentTerm pTerm = if null errors
                           then Right Term { termKey = key
                                           , termValues = vals
                                           }
                           else Left $ head errors
  where
    key = decodeUtf16LE $ pTermKey pTerm
    decoded = partitionEithers $ map decode (pTermValues pTerm)
    errors = fst decoded
    vals = snd decoded

putByteString' :: B.ByteString -> Put
putByteString' b =  do
  putWord64le $ (fromIntegral . B.length) b
  putByteString b

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


getByteString' :: Get B.ByteString
getByteString' = do
  size <- getWord64le
  getByteString (fromIntegral size)

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
