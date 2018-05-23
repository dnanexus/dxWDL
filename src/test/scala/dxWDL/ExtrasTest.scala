package dxWDL

import com.dnanexus.AccessLevel
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class ExtrasTest extends FlatSpec with Matchers {
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
            Extras.parse(ex1.parseJson)
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
            Extras.parse(ex2.parseJson)
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
            Extras.parse(ex3.parseJson)
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
        val extras = Extras.parse(js)
        extras.defaultTaskDxAttributes should be (Some(DxRunSpec(Some(DxExecPolicy(Some(Map("*" -> 3)),
                                                                                   None)),
                                                                 Some(DxTimeout(None,
                                                                                Some(12),
                                                                                None)),
                                                                 Some(DxAccess(Some(Vector("*")),
                                                                               Some(AccessLevel.CONTRIBUTE),
                                                                               Some(AccessLevel.VIEW),
                                                                               Some(true),
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
        val extras = Extras.parse(js)

        val restartPolicy = Map("UnresponsiveWorker" -> 2, "JMInternalError" -> 0, "ExecutionError" -> 4)
        extras.defaultTaskDxAttributes should be (Some(DxRunSpec(
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
            Extras.parse(js)
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

}
