task BadCommand {
      command {
          ls /xx/yyy
      }
      runtime {
          docker: "broadinstitute/genomes-in-the-cloud:2.2.4-1469632282"
      }
      output {
          Int rc = 1
      }
}

workflow bad_status {
    call BadCommand
    output {
        Int rc = BadCommand.rc
    }
}
