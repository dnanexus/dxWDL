version 1.0

workflow test_wf {
   input {
     File f
     String s = basename(f)
   }

   call test_task1 { input: s = s }

   call test_task2 { input: s = s }

   output {
     String x1 = test_task1.x
     String x2 = test_task2.x
   }
}

task test_task1 {
  input {
    String s
  }

  command {}

  output {
    String x = s
  }

   runtime {
     docker: "ubuntu"
   }
}

task test_task2 {
  input {
    String s
  }

  command {}

  output {
    String x = s
  }

   runtime {
     docker: "ubuntu"
   }
}
