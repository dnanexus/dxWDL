version 1.0

struct WordStruct {
    String word
    File? catalog
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


workflow type_mismatch {
    WordStruct bamboo = {"word" : "bamboo"}
    call red_panda {
        input:
            sentence = [bamboo],
            numbers = [1, 4]
    }
}
