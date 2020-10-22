version 1.0

import "imports/subworkflow.wdl"
import "imports/lib.wdl"

workflow multi_import {
  call subworkflow.foo
  call lib.bar
}