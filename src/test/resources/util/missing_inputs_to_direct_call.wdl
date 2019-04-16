version 1.0

task sum_012 {
  input {
    Int? a
    Int? b
  }
  command {}
  output {
    Int result = 0
  }
}

workflow missing_inputs_to_direct_call {
    input {
    }

    call sum_012 as sum_var1
    call sum_012 as sum_var2 { input: a=3 }
    call sum_012 as sum_var3 { input: a=3, b=10 }

    output {
    }
}
