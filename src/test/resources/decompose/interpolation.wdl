workflow interpolation {
    String fruit = "lemon"
    String ice = "slush"

    scatter (s in ["G", "C", "T", "A"]) {
        call Concat as ac1 {
              input:
                  x = "${s}.S16",
                  y = "${s}.none"
         }

        call Concat as ac2 {
            input:
                x = "Tamara likes ${fruit}s and ${ice}",
                y = "Would you like some too?"
        }
    }

    output {
        Array[String] r1 = ac1.result
        Array[String] r2 = ac2.result
    }
}

# Concatenate two string
task Concat {
    String x
    String y

    command {
        echo ${x}_${y}
    }
    output {
        String result = read_string(stdout())
    }
}
