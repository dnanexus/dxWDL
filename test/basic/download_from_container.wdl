version 1.0


workflow download_from_container {
    input {
        Boolean flag = true
    }

    if (flag) {
        call create_file

        call sort_names {
            input : names = create_file.result
        }
    }

    output {
        File? result = sort_names.result
    }
}

task create_file {
    command {
        echo "James" > name_list.txt
        echo "Hanna" >> name_list.txt
        echo "Lena" >> name_list.txt
        echo "Jennifer" >> name_list.txt
    }
    output {
        File result = "name_list.txt"
    }
}

task sort_names {
    input {
        File names
    }
    command {
        sort ~{names} > name_list.sorted.txt
    }
    output {
      File result = "name_list.sorted.txt"
    }
}
