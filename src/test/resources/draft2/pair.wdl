workflow multiple_imports {
  # accessing members of a pair structure
  Pair[Int, Int] p = (5, 8)

  call mul {
    input: a=p.left, b=p.right
  }

  output {
    Int x = mul.x
  }
}

task mul {
  Int a
  Int b

  command {}

  output {
    Int x = a * b
  }

  runtime {
    docker: "ubuntu"
  }
}