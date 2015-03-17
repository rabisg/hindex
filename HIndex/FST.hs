module HIndex.FST where

import           HIndex.Types

import qualified Data.Map     as M

buildFST :: [(Key, Int)] -> TermFST
buildFST = TermFST . M.fromList

getOffset :: TermFST -> Key -> Maybe Int
getOffset (TermFST a) key = M.lookup key a
