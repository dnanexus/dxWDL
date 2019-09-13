version 1.0

struct WordStruct {
  String word
}

task type_mismatch {
    input {
        Array[WordStruct]? sentence
        Array[Int]+ numbers
    }
    command {
        echo "hello"
    }
}
