version 1.0

workflow optional_call_output {
  input {
    Boolean makeCall = true
    File defaultFile
  }

  call optional_output

  if (makeCall) {
    File result = select_first([optional_output.out, defaultFile])
    call task2 {
      input: f = result
    }
  }

  output {
    File? out = task2.fout
  }
}

task optional_output {
  command <<< >>>

  output {
    File? out = "foo.txt"
  }
}

task task2 {
  input {
    File f
  }

  command <<<
  >>>

  output {
    File fout = f
  }
}