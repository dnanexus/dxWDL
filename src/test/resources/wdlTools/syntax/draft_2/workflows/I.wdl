workflow biz {
  String s

  output {
    File f = "dummy.txt"
  }

  call bar as boz {
    input: i = s
  }
  scatter (i in [1, 2, 3]) {
    call add { input: x = i }
  }
  if (true == false) {
    call sub
  }

  meta {
    author : "Robert Heinlein"
  }
}
