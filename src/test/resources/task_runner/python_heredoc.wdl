task python_command {
    File A
    File B
  command <<<
python <<CODE
import os
dir_path_A = os.path.dirname("${A}")
dir_path_B = os.path.dirname("${B}")
print((dir_path_A == dir_path_B))
CODE
  >>>
  output {
    String result = read_string(stdout())
  }
}
