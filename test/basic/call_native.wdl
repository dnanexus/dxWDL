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
        Int sum_var1_result = sum_var1.result
        Int sum_var2_result = sum_var2.result
        Int sum_var3_result = sum_var3.result
        Array[Int] mk_list_all = mk_list.all
        String concat_c = concat.c
        Array[Int]+? mk_list2_all = mk_list2.all
        Array[Int] sum_result = sum.result
        Array[Int] sum2_result = sum2.result
        Boolean diff_equality = diff.equality
    }
}
