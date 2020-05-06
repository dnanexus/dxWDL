version 1.0

task bwa {
  command {}
  output {
    Int version = 2
  }
}

task bar {
  input {
    Int a
  }
  command {}
}

workflow foo {
  call bwa
  call bar as bar2 { input : a = bwa.version }
}
