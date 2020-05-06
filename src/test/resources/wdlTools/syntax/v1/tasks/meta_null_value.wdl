version 1.0

task Foo {
  input {
    Int i
  }

  command <<<
    echo ~{i}
  >>>

  parameter_meta {
    i: null
  }
}
