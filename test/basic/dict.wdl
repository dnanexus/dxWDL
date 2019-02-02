version 1.0

task Add {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b
    }
}

task Multiply {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a * b
    }
}

# create a map that has files as sub structures
task createFruit {
    input {}
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
    input {}
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

task makeSalad {
    input {
        Pair[String, String] veggies
        Pair[String, String] fruit
    }
    command {
       echo ~{veggies.left}
       echo ~{veggies.right}
       echo ~{fruit.left}
       echo ~{fruit.right}
    }
    output {
       Array[String] ingredients = read_lines(stdout())
    }
}

workflow dict {
    input {
        Map[String, Int] mSI
    }
#    Map[Int, Int] mII = {1: 10, 2: 11}
#    Map[Int, Float] mIF = {1: 1.2, 10: 113.0}

    call createFruit
    call createMultiFruit

#    scatter (pair1 in mSI) {
#        String valueSI = pair1.left
#    }

#    scatter (pair2 in mII) {
#        Int valueII = pair2.right
#    }

#    scatter (pair3 in mIF) {
#        call Add as add {
#            input: a=pair3.left, b=5
#        }
#    }

    # accessing members of a pair structure
    Pair[Int, Int] p2 = (5, 8)
    call Multiply as mul {
        input: a=p2.left, b=p2.right
    }

    Pair[String, String] v = ("carrots", "oranges")
    Pair[String, String] f = ("pear", "coconut")
    call makeSalad{
        input: veggies=v, fruit=f
    }

    output {
#        Array[String] keysSI = valueSI
#        Array[Int] valuesII = valueII
#        Array[Int] addition = add.result
        Int mul_res = mul.result
    }
}
