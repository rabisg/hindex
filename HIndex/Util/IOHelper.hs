module HIndex.Util.IOHelper where

import           System.Directory
import           System.IO
import           System.Posix.Temp (mkstemp)

withFileIfExists :: FilePath -> a -> IOMode -> (Handle -> IO a) -> IO a
withFileIfExists filePath def ioMode f = do
  exists <- doesFileExist filePath
  if exists then
    withFile filePath ioMode f
    else return def

withTmpFile :: FilePath -> ((FilePath, Handle) -> IO a) -> IO (a, FilePath)
withTmpFile baseDir f = do
  (tmpFilePath, tmpHandle) <- mkstemp baseDir
  res <- f (tmpFilePath, tmpHandle)
  hClose tmpHandle
  return (res, tmpFilePath)
