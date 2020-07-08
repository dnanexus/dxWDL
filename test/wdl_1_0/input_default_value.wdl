version 1.0

workflow input_default_value {
   input {
     File f
     String s = basename(f)
   }

   call test_task { input: s = s }

   output {
     String x = test_task.x
   }
}

task test_task {
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