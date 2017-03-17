# The docker image doesn't exist, this should cause an error
task BadCommand {
      command {
          ls /tmp
      }
      runtime {
          docker: "broadinstitute/genomes-in-tlou.2.4-1469632282"
      }
      output {
          Int rc = 1
      }
}

workflow bad_status2 {
    call BadCommand
    output {
        BadCommand.rc
    }
}
