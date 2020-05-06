version 1.0

#
import "tools/count_dogs.wdl" as cd

workflow foo {
  call cd.count_dogs { input : dog_breeds = "all_known_dogs.txt" }
}
