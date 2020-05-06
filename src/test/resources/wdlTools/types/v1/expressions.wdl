version 1.0

workflow foo {

  Pair[String, Int] p = ("hello", 1)
  String s1 = p.left
  Int i = p.right
}
