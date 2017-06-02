task Concat {
    Array[Int] ia
    Array[Float] fa
    Array[Boolean] ba
    Array[String] sa

    command <<<
        echo ${sep=' I=' ia}
        echo ${sep=' F=' fa}
        echo ${sep=' B=' ba}
        echo ${sep=' S=' sa}
    >>>
    output {
        String result = read_string(stdout())
    }
 }

 workflow X {
    Array[Int] ia
    Array[Float] fa
    Array[Boolean] ba
    Array[String] sa

     call Concat {
         input : ia=ia, fa=fa, ba=ba, sa=sa
     }
     output {
         Concat.result
     }
 }
