workflow just_fail_wf {
  call just_fail
}

task just_fail {
  command {
    exit 1
  }
}
