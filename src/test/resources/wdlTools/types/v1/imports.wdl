version 1.0

import "library.wdl" as lib
import "census.wdl" as lib2

workflow w {

  # calls to imports
  call lib.concat as concat { input : a = "He prefers yellow", b = " to green" }
  call lib.add { input : a = 3, b = 5 }
  call lib.gen_array { input : len = 10 }

  # access results
  String longStr = concat.result
  Int i = add.result
  Array[Int] a = gen_array.result

  call lib2.census
  Person p = census.p

  String name = p.name
}
