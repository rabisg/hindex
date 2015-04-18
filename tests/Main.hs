module Main (main) where

import qualified Test.Operations as Operations
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Tests" [Operations.tests]
