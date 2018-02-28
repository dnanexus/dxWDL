import "dx_extern.wdl" as lib

workflow call_native {
    Int a
    String s
    Boolean flag
    File fileA
    File fileB

    call lib.native_sum_012 as sum_var1
    call lib.native_sum_012 as sum_var2 { input: a=3 }
    call lib.native_sum_012 as sum_var3 { input: a=3, b=10 }

    call lib.native_mk_list as mk_list {
        input: a=a, b=5
    }
    call lib.native_concat as concat {
        input: a=s, b="_zebra_fish"
    }

    if (flag) {
        call lib.native_mk_list as mk_list2 {
            input: a=a, b=a
        }
    }
    scatter (i in [1,2,3]) {
        call lib.native_sum as sum { input: a=i, b=i }
        call lib.native_sum as sum2 { input: a=i }
    }
    call lib.native_diff as diff {
        input: a=fileA, b=fileB
    }

    output {
        sum_var1.result
        sum_var2.result
        sum_var3.result
        mk_list.all
        concat.c
        mk_list2.all
        sum.result
        sum2.result
        diff.equality
    }
}
