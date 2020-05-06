version 1.0

workflow foo {

  Array[Pair[File, File]] x = zip([1, 2, 3], ["a", "b", "c"])
  Array[Pair[File, File]] y = cross([1, 2, 3], ["a", "b", "c"])
}
