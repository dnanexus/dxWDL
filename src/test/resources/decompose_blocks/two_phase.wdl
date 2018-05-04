workflow two_phase {
    Int len = 4
    Boolean condA = true
    Boolean condB = true
    Array[Int] xa = [1, 4, 9, 16]

    if ( len > 0 ) {
	if ( !condA ) {
	    scatter (x in xa) {
		call inc { input : a=x}
            }
	}
	if ( condB ) {
	    scatter (x in xa) {
                call add { input: a=x, b=3 }
            }
        }
    }

    output {
        Array[Int]? incs = inc.result
        Array[Int]? adds = add.result
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
