workflow dict {
    Map[String, Int] mSI = {"a": 1, "b": 2}

    scatter(pair in mSI) {
      String value = pair.left
    }
    output {
      Array[String] keys = value
    }
}
