version 1.0

workflow scatter_with_eval {
  Array[String] names = ["Michael", "Lukas", "Martin", "Shelly", "Amy"]
  Array[Int] ages = [27, 9, 13, 67, 2]

  scatter ( pair in zip(names, ages)) {
    String info = pair.left + "_" + pair.right
  }

  output {
    Array[String] result = info
  }
}
