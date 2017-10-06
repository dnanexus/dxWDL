task AddNumbers {
    Int x
    Int y

    command {
        echo $((x + y))
    }
    output {
        Int result = read_int(stdout())
    }
}

workflow hello_world {
   Int a
   Int b

   call AddNumbers {
       input: x = a, y = b
   }
   output {
       AddNumbers.result
   }
}