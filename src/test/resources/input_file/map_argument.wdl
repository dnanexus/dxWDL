version 1.0

# check that maps work

workflow map_argument {
    input {
        Map[String, Int] names_and_phones
    }

    output {
        Map[String, Int] result = names_and_phones
    }
}
