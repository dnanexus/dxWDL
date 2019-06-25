version 1.0

struct WordStruct {
  String word
}

task echo_xx {
  input {
    WordStruct input_word
  }
  command <<<
    echo ~{input_word.word}
  >>>
  runtime {
    docker: "debian:stretch-slim"
  }
  output {
    String out = read_string(stdout())
  }
}

workflow array_structs {
  input {
    Array[WordStruct] words_to_say
  }
  scatter (input_word in words_to_say) {
    call echo_xx {
      input:
        input_word = input_word
    }
  }
  output {
    Array[String] out_words = echo_xx.out
  }
}
