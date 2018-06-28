workflow hello {
    String? empty
    String? s = "world"

    call hello_A as ha1
    call hello_A as ha2 { input: who = empty }
    call hello_A as ha3 { input: who = "Porcupine" }

    call hello_B as hb1 { input: who = s }

    call hello_C as hc1
    call hello_C as hc2 { input: who = empty }
    call hello_C as hc3 { input: who = "Porcupine" }

    output {
        String ra1 = ha1.result
        String ra2 = ha2.result
        String ra3 = ha3.result
        String rb1 = hb1.result
        String rc1 = hc1.result
        String rc2 = hc2.result
        String rc3 = hc3.result
    }
}

task hello_A {
    String? who = "world"

    command {
        echo "Hello, ${who}"
    }
    output {
        String result = read_string(stdout())
    }
}


task hello_B {
    String who

    command {
        echo "Hello, ${who}"
    }
    output {
        String result = read_string(stdout())
    }
}


task hello_C {
    String? who
    String who_actual = select_first([who, "dave.jones"])

    command {
        echo "Hello, ${who_actual}"
    }
    output {
        String result = read_string(stdout())
    }
}
