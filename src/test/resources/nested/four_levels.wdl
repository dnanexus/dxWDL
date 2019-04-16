version 1.0

workflow four_levels {
    input {
        String username
    }

    if (username == "a") {
        call concat as c1 { input: a = "x", b = username }
        call concat as c2 { input: a = c1.result, b = c1.result}
    }

    scatter (i in [1, 4, 9]) {
        scatter (j in ["john", "clease"]) {
            if (j == "clease") {
                if (i == 4) {
                    Int z = 1
                }
            }

            if (j == "john") {
                call concat as c3 { input: a = j, b = "banana" }
            }
        }
    }

    output {
        String? r1 = c2.result
        Array[Array[Int?]] r2 = z
    }
}



task concat {
    input {
        String a
        String b
    }
    command {}
    output {
        String result = a + "_" + b
    }
}
