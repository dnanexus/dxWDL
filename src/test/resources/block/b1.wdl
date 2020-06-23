version 1.0

workflow foo {
  Int i = 3

  scatter (j in ["john", "clease"]) {
    if (j == "clease") {
      if (i == 4) {
        Int z = 1
      }
    }

    if (j == "john") {
      call concat as c3 { input: a = j, b = "banana" }
    }
  }
}

task concat {
    input {
        String a
        String b
    }
    command {}
    output {
        String result = a + "_" + b
    }
}
