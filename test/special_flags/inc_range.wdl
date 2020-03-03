version 1.0

import "add2.wdl" as A

workflow inc_range {

    scatter (item in range(3)) {
        call A.add2 as add { input: a=1, b=item }
    }

    output {
        Array[Int] result = add.c
    }
}
