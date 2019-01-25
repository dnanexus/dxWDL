package dxWDL

import com.dnanexus.AccessLevel
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wdl.draft2.model.WdlExpression
import wom.values.WomString

class ExtrasTest extends FlatSpec with Matchers {
    val verbose = Verbose(true, true, Set.empty)


    it should "recognize restartable entry points" in {
        val runtimeAttrs : JsValue =
            """|{
               |  "default_task_dx_attributes" : {
               |     "runSpec" : {
               |       "restartableEntryPoints": "all"
               |     }
               |  }
               |}""".stripMargin.parseJson

        val extras = Extras.parse(runtimeAttrs, verbose)
        extras.defaultTaskDxAttributes should be (Some(DxRunSpec(None, None, Some("all"), None)))
    }

    it should "invalid runSpec I" in {
        val ex1 =
            """|{
               | "default_task_dx_attributes" : {
               |     "timeoutPolicy": {
               |        "main": {
               |          "hours": 12
               |        }
               |     }
               |  }
               |}
               |""".stripMargin

        assertThrows[Exception] {
            Extras.parse(ex1.parseJson, verbose)
        }
    }


    it should "invalid runSpec II" in {
        val ex2 =
            """|{
               | "default_task_dx_attributes" : {
               |   "runSpec": {
               |     "timeoutPolicy": {
               |        "main": {
               |          "hours": 12
               |        }
               |     }
               |   }
               |  }
               |}
               |""".stripMargin

        assertThrows[Exception] {
            Extras.parse(ex2.parseJson, verbose)
        }
    }

    it should "invalid runSpec III" in {
        val ex3 =
            """|{
               | "default_task_dx_attributes" : {
               |   "runSpec": {
               |     "access" : {
               |        "project": "CONTRIBUTE__XD",
               |        "allProjects": "INVAL",
               |        "developer": true
               |     }
               |   }
               |  }
               |}
               |""".stripMargin

        assertThrows[Exception] {
            Extras.parse(ex3.parseJson, verbose)
        }
    }


    it should "parse the run spec" in {
        val runSpecValid =
            """|{
               | "default_task_dx_attributes" : {
               |   "runSpec": {
               |     "executionPolicy": {
               |        "restartOn": {
               |          "*": 3
               |        }
               |     },
               |     "timeoutPolicy": {
               |        "*": {
               |          "hours": 12
               |        }
               |     },
               |     "access" : {
               |        "project": "CONTRIBUTE",
               |        "allProjects": "VIEW",
               |        "network": [
               |           "*"
               |         ],
               |        "developer": true
               |     }
               |   }
               |  }
               |}
               |""".stripMargin

        val js = runSpecValid.parseJson
        val extras = Extras.parse(js, verbose)
        extras.defaultTaskDxAttributes should be (Some(DxRunSpec(
                                                           Some(DxAccess(Some(Vector("*")),
                                                                         Some(AccessLevel.CONTRIBUTE),
                                                                         Some(AccessLevel.VIEW),
                                                                         Some(true),
                                                                         None)),
                                                           Some(DxExecPolicy(Some(Map("*" -> 3)),
                                                                             None)),
                                                           None,
                                                           Some(DxTimeout(None,
                                                                          Some(12),
                                                                          None))
                                                       )))
    }


    it should "parse complex execution policy" in {
        val runSpec =
            """|{
               | "default_task_dx_attributes" : {
               |   "runSpec": {
               |     "executionPolicy": {
               |        "restartOn": {
               |           "UnresponsiveWorker": 2,
               |           "JMInternalError": 0,
               |           "ExecutionError": 4
               |        },
               |        "maxRestarts" : 5
               |     }
               |    }
               |  }
               |}
               |""".stripMargin

        val js = runSpec.parseJson
        val extras = Extras.parse(js, verbose)

        val restartPolicy = Map("UnresponsiveWorker" -> 2, "JMInternalError" -> 0, "ExecutionError" -> 4)
        extras.defaultTaskDxAttributes should be (Some(DxRunSpec(
                                                           None,
                                                           Some(DxExecPolicy(Some(restartPolicy), Some(5))),
                                                           None,
                                                           None)
                                                  ))
    }

    it should "recognize error in complex execution policy" in {
        val runSpec =
            """|{
               | "default_task_dx_attributes" : {
               |   "runSpec": {
               |     "executionPolicy": {
               |        "restartOn": {
               |           "UnresponsiveWorker_ZZZ": 2,
               |           "ExecutionError": 4
               |        },
               |        "maxRestarts" : 5
               |     }
               |    }
               |  }
               |}
               |""".stripMargin

        val js = runSpec.parseJson
        assertThrows[Exception] {
            Extras.parse(js, verbose)
        }
    }


    it should "generate valid JSON execution policy" in {
        val expectedJs : JsValue =
            """|{
               | "executionPolicy": {
               |    "restartOn": {
               |       "*": 5
               |    },
               |    "maxRestarts" : 4
               | }
               |}
               |""".stripMargin.parseJson

        val execPolicy = DxExecPolicy(Some(Map("*" -> 5)),
                                      Some(4))
        JsObject(execPolicy.toJson) should be(expectedJs)
    }

    it should "generate valid JSON timeout policy" in {
        val expectedJs : JsValue =
            """|{
               |  "timeoutPolicy": {
               |     "*": {
               |        "hours": 12,
               |        "minutes" : 30
               |     }
               |  }
               |}
               |""".stripMargin.parseJson

        val timeout = DxTimeout(None, Some(12), Some(30))
        JsObject(timeout.toJson) should be(expectedJs)
    }

    it should "handle default runtime attributes" in {
        val runtimeAttrs : JsValue=
            """|{
               | "default_runtime_attributes" : {
               |     "docker" : "quay.io/encode-dcc/atac-seq-pipeline:v1",
               |     "zones": "us-west1-a us-west1-b us-west1-c us-central1-c us-central1-b",
               |     "failOnStderr" : false,
               |     "continueOnReturnCode" : 0,
               |     "preemptible": "0",
               |     "bootDiskSizeGb": "10",
               |     "noAddress": "false"
               | }
               |}""".stripMargin.parseJson

        val extras = Extras.parse(runtimeAttrs, verbose)
        val dockerOpt: Option[WdlExpression] = extras.defaultRuntimeAttributes.get("docker")
        dockerOpt match {
            case None =>
                throw new Exception("Wrong type for dockerOpt")
            case Some(docker) =>
                Utils.evalConst(docker) should equal (WomString("quay.io/encode-dcc/atac-seq-pipeline:v1"))
        }
    }

    it should "handle default runtime attributes that are empty" in {
        val rtEmpty : JsValue =
            """|{
               | "default_runtime_attributes" : {
               |     "preemptible": "0",
               |     "bootDiskSizeGb": "10"
               | }
               |}""".stripMargin.parseJson

        val extrasEmpty = Extras.parse(rtEmpty, verbose)
        extrasEmpty.defaultRuntimeAttributes should equal(Map.empty)
    }

    it should "accept per task attributes" in {
        val runSpec : JsValue =
            """|{
               | "default_task_dx_attributes" : {
               |   "runSpec": {
               |     "timeoutPolicy": {
               |        "*": {
               |          "hours": 12
               |        }
               |     }
               |   }
               |  },
               | "per_task_dx_attributes" : {
               |   "Add": {
               |      "runSpec": {
               |        "timeoutPolicy": {
               |          "*": {
               |             "minutes": 30
               |          }
               |        }
               |      }
               |    },
               |    "Multiply" : {
               |      "runSpec": {
               |        "timeoutPolicy": {
               |          "*": {
               |            "minutes": 30
               |          }
               |        },
               |        "access" : {
               |          "project": "UPLOAD"
               |        }
               |      }
               |    }
               |  }
               |}
               |""".stripMargin.parseJson

        val extras = Extras.parse(runSpec, verbose)
        extras.defaultTaskDxAttributes should be (
            Some(DxRunSpec(
                     None,
                     None,
                     None,
                     Some(DxTimeout(None, Some(12), None))
                 )))
        extras.perTaskDxAttributes should be (
            Map("Multiply" -> DxRunSpec(Some(DxAccess(None, Some(AccessLevel.UPLOAD), None, None, None)),
                                        None, None, Some(DxTimeout(None, None, Some(30)))),
                "Add" -> DxRunSpec(None, None, None, Some(DxTimeout(None, None, Some(30)))))
        )
    }

    it should "parse the docker registry section" in {
        val data =
            """|{
               | "docker_registry" : {
               |   "registry" : "foo.bar.dnanexus.com",
               |   "username" : "perkins",
               |   "credentials" : "The Bandersnatch has gotten loose"
               | }
               |}
               |""".stripMargin.parseJson

        val extras = Extras.parse(data, verbose)
        extras.dockerRegistery should be (
            Some(DockerRegistery(
                     "foo.bar.dnanexus.com",
                     "perkins",
                     "The Bandersnatch has gotten loose")))
    }

    it should "recognize errors in docker registry section" in {
        val data =
            """|{
               | "docker_registry" : {
               |   "registry_my" : "foo.bar.dnanexus.com",
               |   "username" : "perkins",
               |   "credentials" : "BandersnatchOnTheLoose"
               | }
               |}
               |""".stripMargin.parseJson
        assertThrows[Exception] {
            Extras.parse(data, verbose)
        }
    }

    it should "recognize errors in docker registry section II" in {
        val data =
            """|{
               | "docker_registry" : {
               |   "registry" : "foo.bar.dnanexus.com",
               |   "credentials" : "BandersnatchOnTheLoose"
               | }
               |}
               |""".stripMargin.parseJson
        assertThrows[Exception] {
            Extras.parse(data, verbose)
        }
    }


    it should "recognize errors in docker registry section III" in {
        val data =
            """|{
               | "docker_registry" : {
               |   "creds" : "XXX"
               | }
               |}
               |""".stripMargin.parseJson
        assertThrows[Exception] {
            Extras.parse(data, verbose)
        }
    }
}
