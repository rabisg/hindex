{-# LANGUAGE RecordWildCards #-}
module HIndex.Metadata where

import           HIndex.Constants
import           HIndex.Types
import           HIndex.Util.BinaryHelper
import           HIndex.Util.IOHelper

import           Data.Binary
import           Data.Binary.Get          (Decoder (..), getWord32le,
                                           pushChunks, runGetIncremental)
import           Data.Binary.Put          (putWord32le, runPut)
import qualified Data.ByteString.Lazy     as LB
import           System.Directory         (renameFile)
import           System.FilePath.Posix    ((</>))
import           System.IO                (IOMode (..))

data Metadata = Metadata { metaNextSegN   :: Int
                         , metaActiveSegs :: [Int]
                         }
                deriving (Show)

instance Binary Metadata where
  put Metadata{..} = do
    putWord32le (fromIntegral metaNextSegN)
    putListOf putWord32le (map fromIntegral metaActiveSegs)
  get = do
    nextSegN <- getWord32le
    activeSegs <- getListOf getWord32le
    return Metadata { metaNextSegN = fromIntegral nextSegN
                    , metaActiveSegs = map fromIntegral activeSegs
                    }

writeMetadata :: HIndexConfig -> Metadata -> IO ()
writeMetadata conf metadata = do
  (_, tmpFilePath) <- withTmpFile baseDir $
                      \(_, handle) -> LB.hPut handle $ runPut (put metadata)
  renameFile tmpFilePath metaFilePath
  where
    baseDir = hBaseDirectory conf
    metaFilePath = baseDir </> metadataFileName

readMetadata :: HIndexConfig -> IO Metadata
readMetadata conf = withFileIfExists metaFilePath defMetadata ReadMode readMetadataFromFile
  where
    baseDir = hBaseDirectory conf
    metaFilePath = baseDir </> metadataFileName
    defMetadata = Metadata 1 []
    readMetadataFromFile handle = do
      bs <- LB.hGetContents handle
      case runGetIncremental get `pushChunks` bs of
       Done _ _ metadata -> return metadata
       _ -> fail "Could not read metadata"
