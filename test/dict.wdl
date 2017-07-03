# Testing map functions
import "library_math.wdl" as lib

workflow dict {
    Map[String, Int] mSI = {"a": 1, "b": 2}
    Map[Int, Int] mII = {1: 10, 2: 11}
    Map[Int, Float]  mIF = {1: 1.2, 10: 113.0}
    Map[String, Boolean] mSB = {"x": true, "y": false, "z": true}

    scatter(pair in mSI) {
        String valueSI = pair.left
    }

    scatter(pair in mII) {
        Int valueII = pair.right
    }

    scatter(pair in mIF) {
        Int x = pair.left
        call lib.Add as add {
            input: a=x, b=5
        }
    }

# This should cause a compilation failure
#    Int xtmp5=99

    output {
      Array[String] keysSI = valueSI
      Array[Int] valuesII = valueII
      Array[Int] addition = add.result
#      value
    }
}
