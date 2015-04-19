module HIndex ( delete
              , initIndex
              , put
              , flush
              , get
              , getConfig
              , HIndexConfig(..)
              , HIndex()
              , HIndexValue
              , TermValue(..)
              , HIndexDocument(..)
              ) where

import           HIndex.Init
import           HIndex.Operations
import           HIndex.Types

getConfig :: HIndex a b -> HIndexConfig
getConfig = hConfig
