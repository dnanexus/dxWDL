workflow hello {
    call hello_A as h1
    String? empty
    call hello_A as h2 { input: who = empty }
    call hello_A as h3 { input: who = "Porcupine" }

    String? s = "world"
    call hello_B as hb { input: who = s }

    output {
        String r1 = h1.result
        String r2 = h2.result
        String r3 = h3.result
        String rb = hb.result
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
