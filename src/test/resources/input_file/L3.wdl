import "L2.wdl" as lib

workflow L3 {
    Int x
    Int y

    call lib.L2 { input: x = x, y = y }
}
