workflow stdlib_variables {
    Array[Int]+ numbers
    Array[String]+ ranges

    scatter (sub in numbers) {
        call add {input: a=4, b=sub}
    }

    scatter (range in ranges) {
        call concat {input: x="___", y=range}
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

task concat {
    String x
    String y
    command {
    }
    output {
        String result = x + "_" + y
    }
}
