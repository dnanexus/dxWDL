# Testing map functions
import "library_math.wdl" as lib

# create a map that has files as sub structures
task createFruit {
    command <<<
      echo "Apple" > A.txt
      echo "Mellon" > M.txt
    >>>
    output {
        Map[String, File] m = {"Apple": "A.txt", "Mellon": "M.txt"}
    }
}

# construct a map that uses an array of files
task createMultiFruit {
    command <<<
      echo "Pear" > P.txt
      echo "Tomato" > T.txt
      echo "Cherry" > C.txt
      echo "Carrots" > C2.txt
    >>>
    output {
        Map[String, Array[File]] m = {"P": ["P.txt"], "T": ["T.txt"], "C": ["C.txt", "C2.txt"]}
    }
}


workflow dict {
    Map[String, Int] mSI = {"a": 1, "b": 2}
    Map[Int, Int] mII = {1: 10, 2: 11}
    Map[Int, Float]  mIF = {1: 1.2, 10: 113.0}
    Pair[Int, String] p = (3, "carrots and oranges")

    call createFruit
    call createMultiFruit

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
      Map[String, File] cfM = createFruit.m
#      value
    }
}
