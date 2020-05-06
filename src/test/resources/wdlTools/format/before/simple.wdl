version 1.0

  task foo {
input {
    # This is a comment
        # that should be reformatted - it is very long and so should be wrapped at 100 characters
        String s
        Int i
}

    String x = "${s}.txt" # This is an in-line comment
      String y = "foo"
    Int z =
      i+ i +i # Todo: the formatter shouldn't add parens
    Int a = if i>1 then 2
    else 3


    command <<<
    echo ~{x}
    echo ~{i * i}
    echo ~{ if true then 'a'
    else 'b'}
    >>>

    output {
        String sout = x
    }


    runtime {
        docker: "debian:stretch-slim"
    }
}