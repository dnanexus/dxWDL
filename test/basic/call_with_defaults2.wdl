workflow call_with_defaults2 {
    String? who

    call username as u1 { input: who = who }
    call username as u2
    call username as u3 { input: who = "Sandy" }

    output {
        String r1 = u1.result
        String r2 = u2.result
        String r3 = u3.result
    }
}

task username {
    String? who = "world"

    command {
        echo "Hello, ${who}"
    }
    output {
        String result = read_string(stdout())
    }
}
