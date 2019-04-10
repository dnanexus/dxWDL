version 1.0

workflow four_levels {
    input {
        String username
    }

    if (username == "a") {
        call zconcat as c1 { input: a = "x", b = username }
    }

    scatter (name_part in ["john", "clease"]) {
        if (name_part == "clease") {
            if (2 < 10) {
                Int z = 1
                call zconcat as c2 { input: a = "hello", b = name_part }
            }
        }

        if (name_part == "john") {
            call zconcat as c3 { input: a = name_part, b = username }
        }
    }

    output {
        String? r1 = c1.result
        Array[String?] r2 = c2.result
        Array[String?] r3 = c3.result
    }
}



task zconcat {
    input {
        String a
        String b
    }
    command {}
    output {
        String result = a + "_" + b
    }
}
