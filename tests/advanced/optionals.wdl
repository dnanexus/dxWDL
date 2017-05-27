task ident {
    Int? num
    command {
    }
    output {
      Int? result = num
    }
}

workflow optionals {
    Int? i

    call ident as I1 { input: num=i }
    call ident as I2 { input: num=I1.result }

    output {
        I2.result
    }
}
