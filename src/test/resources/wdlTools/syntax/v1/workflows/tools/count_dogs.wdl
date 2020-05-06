version 1.0

import "library.wdl" as lib

workflow count_dogs {
  input {
    File dog_breeds
  }
  call lib.wc { input : f = dog_breeds }
  output {
    Int result = read_string(stdin())
  }
}
