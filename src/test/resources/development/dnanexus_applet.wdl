
version development

workflow native_sum_wf {
    input {
        Int a
        Int b
    }

    call native_sum_012 {
        input:
            a=a,
            b=b
    }

    output {
        Int result = native_sum_012.result
    }
}


task native_sum_012 {
  input {
    Int? a
    Int? b
  }
  command {}
  output {
    Int result = 0
  }
  meta {
     type : "native"
     id : "applet-FX028xj0ffP043xz5378JyB7"
  }
}
