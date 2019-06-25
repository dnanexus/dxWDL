version 1.0

task echo_this_pair {
  input {
    String first_word
    String second_word
  }
  command <<<
  set -exo

  echo ~{first_word} >> output.txt
  echo ~{second_word} >> output.txt
  >>>
  runtime {
    docker: "debian:stretch-slim"
  }
  output {
    String echoed_words = read_string("output.txt")
  }
}

workflow echo_pairs {
  input {
    Array[Pair[String, String]] pairs_of_words
  }
  scatter (pair in pairs_of_words) {
    call echo_this_pair {
      input:
        first_word = pair.left,
        second_word = pair.right
    }
  }
  output {
    Array[String] all_echoed_words = echo_this_pair.echoed_words
  }
}
