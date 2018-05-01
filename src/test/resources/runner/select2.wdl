workflow select2 {
    Int? x
    Int y  = select_first([x,20])

    output {
        Int out_y = y
    }
}
