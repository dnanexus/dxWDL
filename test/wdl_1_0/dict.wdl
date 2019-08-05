version 1.0

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

    call createFruit
    call createMultiFruit

    Pair[String, String] v = ("carrots", "oranges")
    Pair[String, String] f = ("pear", "coconut")
    call makeSalad{
        input: veggies=v, fruit=f
    }

    output {
        Array[String] result = makeSalad.ingredients
    }
}
