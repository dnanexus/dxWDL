# Testing map functions
import "library_math.wdl" as lib

workflow dict {
    Map[String, Int] mSI = {"a": 1, "b": 2}

#    Map[Int, Int] mII = {1: 10, 2: 11}
#    Map[Int, Float]  mIF = {1: 1.2, 10: 113}
#    Map[String, Boolean] = {"x": true, "y": false, "z": true}

    scatter(pair in mSI) {
      String value = pair.left
    }
    output {
#      Array[String] keys = value
      value
    }
}
