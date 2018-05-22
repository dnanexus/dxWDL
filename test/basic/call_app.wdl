task testing_app_yhwang_1 {
    Int int
    command {
    }
    output {
        Int int_out = 0
    }
    meta {
        type: "native"
        id: "app-F308vPQ0kff0kZ716ZjV5P7g"
    }
}

workflow call_app {
    call testing_app_yhwang_1 { input: int=3}
}
