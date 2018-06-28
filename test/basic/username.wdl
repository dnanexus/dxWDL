workflow username_wf {
    String? who

    call username { input: who = who }
}

task username {
    String? who = "world"

    command {
        echo "Hello, ${who}"
    }
}
