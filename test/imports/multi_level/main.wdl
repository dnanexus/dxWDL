version 1.0


import "util/util.wdl" as util


workflow some_workflow {
    input {
        String file_name
    }
    output {
	File some_file = some_task.some_file
    }
    call util.some_task as some_task {
        input:
                file_name=file_name,
    }
}
