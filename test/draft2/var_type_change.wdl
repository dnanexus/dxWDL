# The type of workflow input "fruit" is Array[String]. However,
# a call is made to a task PTAsays where "fruit" has type String.
# This can confuse the decomposition algorithm
#
workflow var_type_change {
  Array[String] fruit = ["Banana", "Apple"]
  Array[Int] indices = range(length(fruit))

  scatter (index in indices) {
    call PTAsays {
      input:
        fruit = fruit[index],
        y = " is good to eat"
    }

    call Add {
      input:  a = 2, b = 4
    }
  }

  output {
     Array[String] result = PTAsays.result
  }
}

task PTAsays {
    String fruit
    String y

    command {
        echo "${fruit} ${y}"
    }
    output {
        String result = read_string(stdout())
    }
}

task Add {
    Int a
    Int b

    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}

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
