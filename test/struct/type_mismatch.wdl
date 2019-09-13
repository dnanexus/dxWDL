version 1.0

struct WordStruct {
    String word
    File catalog
}

task red_panda {
    input {
        Array[WordStruct]? sentence
        Array[Int]+ numbers
    }
    command {
        echo "hello"
    }
}

task create_file {
    command {
        echo "99 balloons go by" > balloon.txt
    }
    output {
        File o = "balloon.txt"
    }
}

workflow type_mismatch {
    call create_file

    WordStruct bamboo = {"word" : "bamboo", "catalog" : create_file.o}
    call red_panda {
        input:
            sentence = [bamboo],
            numbers = [1, 4]
    }
}
