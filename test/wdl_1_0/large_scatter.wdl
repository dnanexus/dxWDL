version 1.0

workflow large_scatter {
  input {
    Int n
  }

  scatter (i in range(n)) {
    call echo { input: i = i }
  }

  call sum_ints {
    input: ints = echo.j
  }

  output {
    Int sum = sum_ints.sum
  }
}

task echo {
  input {
    Int i
  }

  command <<<
  echo ~{i}
  >>>

  output {
    Int j = read_int(stdout())
  }

  runtime {
    docker: "ubuntu"
  }
}

task sum_ints {
  input {
    Array[Int] ints
  }

  command <<<
    echo "~{sep="+" ints}" | bc
  >>>

  output {
    Int sum = read_int(stdout())
  }

  runtime {
    docker: "ubuntu"
  }
}