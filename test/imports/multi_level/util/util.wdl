version 1.0

import "other.wdl" as other

workflow some_task {
    input {
        String file_name
    }
    output {
      File some_file = some_other_task.some_file
    }
    call other.some_other_task as some_other_task {
        input:
		file_name = file_name
    }
}
