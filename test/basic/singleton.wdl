# A file with one task should compile to one runnable
# applet.

task singleton {
    Int a
    Int b
    Int c

    command {
        echo $((${a} + ${b} + ${c}))
    }
    output {
        Int result = read_int(stdout())
    }
}
