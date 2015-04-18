module HIndex ( initIndex
              , put
              , flush
              , getConfig
              , HIndexConfig(..)
              , HIndex()
              , HIndexValue(..)
              ) where

import           HIndex.Init
import           HIndex.Operations
import           HIndex.Types

getConfig :: HIndex a -> HIndexConfig
getConfig = hConfig
