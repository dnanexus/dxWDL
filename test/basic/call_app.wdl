task testing_app_yhwang {
    Int int
    command {
    }
    output {
        Int int_out = 0
    }
    meta {
        type: "native"
        id: "app-F5BXZvj0Q5zJ7z259Vj4Vxxq"
    }
}


workflow call_app {

    call testing_app_yhwang { input: i=3}
}
