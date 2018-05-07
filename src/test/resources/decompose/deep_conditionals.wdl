workflow deep_conditionals {
    Boolean condA
    Boolean condB
    Boolean condC
    Int len = 4
    Array[Int] xa = [1, 4, 9, 16]

    if ( len > 0 ) {
	if ( !condA ) {
	    scatter (x in xa) {
		call inc { input : a=x}
            }
	    scatter (x in xa) {
                call add { input: a=x, b=3 }
            }
            if (condB) {
                call inc as inc2 {input: a = 101}
            }
        }
        if (condA) {
            if (condB) {
                if (condC) {
                    call inc as inc3 {input: a = 101}
                }
            }
        }
    }

    output {
#        Array[Int]? incs = inc.result
#        Array[Int]? adds = add.result
#        Int? i2 = inc2.result
    }
}


task add {
    Int a
    Int b
    command {
    }
    output {
        Int result = a + b
    }
}

task inc {
    Int a

    command {}
    output {
        Int result = a + 1
    }
}
