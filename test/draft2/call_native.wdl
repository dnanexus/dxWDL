#import "dx_extern.wdl" as lib

workflow call_native {
    Int x
    String s
    Boolean flag
    File fileA
    File fileB

    call native_sum_012 as sum_var1
    call native_sum_012 as sum_var2 { input: a=3 }
    call native_sum_012 as sum_var3 { input: a=3, b=10 }

    call native_mk_list as mk_list {
        input: a=x, b=5
    }
    call native_concat as concat {
        input: a=s, b="_zebra_fish"
    }

    if (flag) {
        call native_mk_list as mk_list2 {
            input: a=x, b=x
        }
    }
    scatter (i in [1,2,3]) {
        call native_sum as sum { input: a=i, b=i }
    }
    call native_diff as diff {
        input: a=fileA, b=fileB
    }

    output {
        Int sum_var1_result = sum_var1.result
        Int sum_var2_result = sum_var2.result
        Int sum_var3_result = sum_var3.result
        Array[Int]+ mk_list_all = mk_list.all
        String concat_c = concat.c
        Array[Int]+? mk_list2_all = mk_list2.all
        Array[Int] sum_result = sum.result
        Boolean diff_equality = diff.equality
    }
}

# We currently do not have a code generator for draft-2, so cannot import dx_extern.wdl.
# Instead we just copy the tasks into this file.
task native_sum_012 {
  Int? a
  Int? b

  command {}

  output {
    Int result = 0
  }

  meta {
    type: "native"
    id: "applet-FqZXQXQ0ffP2ZVjVKk4b8Bj9"
  }
}

task native_mk_list {
  Int a
  Int b

  command {}

  output {
    Array[Int]+ all = [0]
  }

  meta {
    type: "native"
    id: "applet-FqZXQX00ffPFZjp9Kjy4vJ19"
  }
}

task native_sum {
  Int? a
  Int? b

  command {}

  output {
    Int result = 0
  }

  meta {
    type: "native"
    id: "applet-FqZXQX80ffP2ZVjVKk4b8Bj7"
  }
}

task native_concat {
  String? a
  String? b

  command {}

  output {
    String c = ""
  }

  meta {
    type: "native"
    id: "applet-FqZXQPQ0ffP62Xb5Kp34bq1J"
  }
}

task native_diff {
  File a
  File b

  command {}

  output {
    Boolean equality = true
  }

  meta {
    type: "native"
    id: "applet-FqZXQQj0ffPKf3b51JPkQZGv"
  }
}