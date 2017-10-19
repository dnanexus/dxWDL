#task Maybe {
#    Int? n
#    command {}
#    output {
#        Int? result = n
#    }
#}

workflow math2 {
#    call Maybe {input: n=2}

#    Array[Int] numbers = [1, 2, Maybe.result]
    Array[Int] numbers = [1, 2]

    output {
        numbers
    }
}
