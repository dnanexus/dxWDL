version 1.0

workflow large_scatter {
  input {
    Int n
  }

  scatter (i in range(n)) {
    call echo { input: i = i }
  }

  output {
    Array[Int] j = echo.j
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