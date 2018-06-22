workflow wf {
    String? who

    call hello { input: who = who }
    call hello as hello2

    output {
        String result = hello.result
        String result2 = hello2.result
    }
}

task hello {
    String? who = "world"

    command {
        echo "Hello, ${who}"
    }
    output {
        String result = read_string(stdout())
    }
}
