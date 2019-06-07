version 1.0

workflow environment_passing_deep_nesting {

    call bear
    call cat

    if (false) {
        call compare { input:
            a = bear.result,
            b = cat.result
        }

        call assert
    }
}


task bear {
    command {}
    output {
        Int result = 10
    }
}

task cat {
  command {}
  output {
      Int result = 3
  }
}

task assert {
    command {}
}


task compare {
    input {
        Int a
        Int b
    }
  command {}
  output {
  }
}
