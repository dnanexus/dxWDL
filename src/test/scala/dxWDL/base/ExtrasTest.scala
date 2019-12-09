package dxWDL.base

import com.dnanexus.AccessLevel
import com.dnanexus.exceptions.ResourceNotFoundException
import dxWDL.compiler.EdgeTest
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.values._
import DefaultJsonProtocol._

class ExtrasTest extends FlatSpec with Matchers {
    val verbose = Verbose(true, true, Set.empty)

    private def getIdFromName(name: String): String = {
        val (stdout, stderr) = Utils.execCommand(s"dx describe ${name} --json")
        stdout.parseJson.asJsObject match {
            case JsObject(x) => JsObject(x).fields("id").convertTo[String]
        }
    }

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
        extras.defaultTaskDxAttributes should be (Some(
                                                      DxAttrs(Some(DxRunSpec(None, None, Some("all"), None)), None)))
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
        extras.defaultTaskDxAttributes should be (
            Some(DxAttrs(Some(DxRunSpec(
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
                              )), None)))
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
        extras.defaultTaskDxAttributes should be (Some(DxAttrs(Some(DxRunSpec(
                                                                        None,
                                                                        Some(DxExecPolicy(Some(restartPolicy), Some(5))),
                                                                        None,
                                                                        None)
                                                               ), None)))
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

    it should "parse the custom_reorg object" in {

        // app_id is mummer nucmer app in project-FJ90qPj0jy8zYvVV9yz3F5gv
        val appId : String = getIdFromName("/release_test/mummer_nucmer_aligner")
        val fileId : String = getIdFromName("Readme.md")

        // inputs is Readme.md file in project-FJ90qPj0jy8zYvVV9yz3F5gv
        val reorg: JsValue   =
            s"""|{
                | "custom_reorg" : {
                |    "app_id" :"${appId}",
                |    "conf" : "${fileId}"
                |  }
                |}
                |""".stripMargin.parseJson

        val extras = Extras.parse(reorg, verbose)
        extras.customReorgAttributes  should be (
            Some(ReorgAttrs(appId, fileId))
        )
    }

    it should "throw IllegalArgumentException due to missing applet id" in {

        val inputs: String = "dx://file-123456"
        val reorg: JsValue   =
            s"""|{
                | "custom_reorg" : {
                |    "conf" : "${inputs}"
                |  }
                |}
                |""".stripMargin.parseJson


        val thrown = intercept[dxWDL.base.IllegalArgumentException] {
            Extras.parse(reorg, verbose)
        }

        thrown.getMessage should be ("app_id must be specified in the custom_reorg section.")
    }

    it should "throw IllegalArgumentException due to missing inputs in custom_reorg section" in {

        // app_id is mummer nucmer app in project-FJ90qPj0jy8zYvVV9yz3F5gv
        val appId : String = getIdFromName("/release_test/mummer_nucmer_aligner")
        val reorg: JsValue   =
            s"""|{
                | "custom_reorg" : {
                |    "app_id" : "${appId}"
                |  }
                |}
                |""".stripMargin.parseJson


        val thrown = intercept[dxWDL.base.IllegalArgumentException] {
            Extras.parse(reorg, verbose)
        }

        //thrown.getMessage should contain  ("inputs must be specified in the custom_reorg section.")
        thrown.getMessage should be  (
            "conf must be specified in the custom_reorg section. Please set the value to null if there is no conf file."
        )
    }

    it should "Allow inputs to be null in custom reorg" in {

        // app_id is mummer nucmer app in project-FJ90qPj0jy8zYvVV9yz3F5gv
        val appId : String = getIdFromName("/release_test/mummer_nucmer_aligner")
        val reorg: JsValue   =
            s"""|{
                | "custom_reorg" : {
                |    "app_id" : "${appId}",
                |    "conf" : null
                |  }
                |}
                |""".stripMargin.parseJson


        val extras = Extras.parse(reorg, verbose)
        extras.customReorgAttributes should be (
            Some(ReorgAttrs(appId, ""))
        )
    }

    it should "throw IllegalArgumentException due to invalid applet ID" in {

        // invalid applet ID
        val appId : String = "applet-123456"
        val reorg : JsValue =
            s"""|{
                |  "custom_reorg" : {
                |      "app_id" : "${appId}",
                |      "conf": null
                |   }
                |}
                |""".stripMargin.parseJson

        val thrown = intercept[dxWDL.base.IllegalArgumentException] {
            Extras.parse(reorg, verbose)
        }

        thrown.getMessage should be  (
            s"dxId must match applet-[A-Za-z0-9]{24}"
        )

    }

    ignore should "throw ResourceNotFoundException due to non-existent applet" in {

        // non-existent (made up) applet ID
        val appId : String = "applet-mPX7K2j0Gv2K2jXF75Bf21v2"
        val reorg : JsValue =
            s"""|{
                |  "custom_reorg" : {
                |      "app_id" : "${appId}",
                |      "conf": null
                |   }
                |}
                |""".stripMargin.parseJson

        val thrown = intercept[Exception] {
            Extras.parse(reorg, verbose)
        }

        thrown.getMessage should be  (
            s"""Error running command dx describe $appId --json"""
        )

    }

    it should "throw IllegalArgumentException due to invalid file ID" taggedAs (EdgeTest) in {

        // app_id is mummer nucmer app in project-FJ90qPj0jy8zYvVV9yz3F5gv

        val appId : String = getIdFromName("/release_test/mummer_nucmer_aligner")
        val inputs : String = "file-1223445"
        val reorg : JsValue =
            s"""|{
                |  "custom_reorg" : {
                |      "app_id" : "${appId}",
                |      "conf": "${inputs}"
                |   }
                |}
                |""".stripMargin.parseJson

//        val thrown = intercept[java.lang.IllegalArgumentException] {
            Extras.parse(reorg, verbose)
//        }

/*        thrown.getMessage should be  (
            s"dxId must match file-[A-Za-z0-9]{24}"
        )*/

    }

    it should "throw ResourceNotFoundException due to non-existant file" in {

        // app_id is mummer nucmer app in project-FJ90qPj0jy8zYvVV9yz3F5gv

        val appId : String = getIdFromName("/release_test/mummer_nucmer_aligner")
        val inputs : String = "dx://file-AZBYlBQ0jy1qpqJz17gpXFf8"
        val reorg : JsValue =
            s"""|{
                |  "custom_reorg" : {
                |      "app_id" : "${appId}",
                |      "conf": "${inputs}"
                |   }
                |}
                |""".stripMargin.parseJson

        val thrown = intercept[ResourceNotFoundException] {
            Extras.parse(reorg, verbose)
        }

        val fileId : String = inputs.replace("dx://", "")

        thrown.getMessage should be  (
            s""""${fileId}" is not a recognized ID"""
        )

    }

    // FIXME: tab should be set to 4 spaces
    ignore should "throw PermissionDeniedException due to applet not having contribute access in the project" in {

        // app_id is sum app in project-FJ90qPj0jy8zYvVV9yz3F5gv
        val appId : String = getIdFromName("/release_test/Sum ")
        val reorg: JsValue =
            s"""|{
                | "custom_reorg" : {
                |    "app_id" : "${appId}",
                |    "conf": "null"
                |  }
                |}
                |""".stripMargin.parseJson

        val thrown = intercept[PermissionDeniedException] {
            Extras.parse(reorg, verbose)

        }

        thrown.getMessage should be (
            s"ERROR: App(let) for custom reorg stage ${appId} does not " +
                s"have CONTRIBUTOR or ADMINISTRATOR access and this is required."
        )
    }

    it should "take app id as well as applet id for custom reorg" in {

        val appId : String = getIdFromName("cloud_workstation")
        val reorg: JsValue =
            s"""|{
                | "custom_reorg" : {
                |    "app_id" : "${appId}",
                |    "conf": null
                |  }
                |}
                |""".stripMargin.parseJson

        val extras = Extras.parse(reorg, verbose)
        extras.customReorgAttributes  should be (
            Some(ReorgAttrs(appId, ""))
        )
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
        val dockerOpt: Option[WomValue] = extras.defaultRuntimeAttributes.m.get("docker")
        dockerOpt match {
            case None =>
                throw new Exception("Wrong type for dockerOpt")
            case Some(docker) =>
                docker should equal (WomString("quay.io/encode-dcc/atac-seq-pipeline:v1"))
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
        extrasEmpty.defaultRuntimeAttributes should equal(WdlRuntimeAttrs(Map.empty))
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
            Some(DxAttrs(Some(DxRunSpec(
                                  None,
                                  None,
                                  None,
                                  Some(DxTimeout(None, Some(12), None))
                              )), None)))
        extras.perTaskDxAttributes should be (
            Map("Multiply" -> DxAttrs(Some(DxRunSpec(Some(DxAccess(None, Some(AccessLevel.UPLOAD), None, None, None)),
                                                     None, None, Some(DxTimeout(None, None, Some(30))))), None),
                "Add" -> DxAttrs(Some(DxRunSpec(None, None, None, Some(DxTimeout(None, None, Some(30))))), None)
            ))
    }

    it should "include optional details and runSpec in per task attributes" in {
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
               |      },
               |      "details": {
               |        "upstreamProjects": [
               |          {
               |            "name": "GATK4",
               |            "repoUrl": "https://github.com/broadinstitute/gatk",
               |            "version": "GATK-4.0.1.2",
               |            "license": "BSD-3-Clause",
               |            "licenseUrl": "https://github.com/broadinstitute/LICENSE.TXT",
               |            "author": "Broad Institute"
               |          }
               |        ]
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
            Some(DxAttrs(Some(DxRunSpec(
                                  None,
                                  None,
                                  None,
                                  Some(DxTimeout(None, Some(12), None))
                              )), None)))

        extras.perTaskDxAttributes should be (
            Map("Add" -> DxAttrs(
                    Some(DxRunSpec(
                             None, None, None, Some(DxTimeout(None, None, Some(30))))),
                    Some(DxDetails(Some(
                                       List(DxLicense(
                                                "GATK4",
                                                "https://github.com/broadinstitute/gatk",
                                                "GATK-4.0.1.2",
                                                "BSD-3-Clause",
                                                "https://github.com/broadinstitute/LICENSE.TXT",
                                                "Broad Institute")))))),
                "Multiply" -> DxAttrs(
                    Some(DxRunSpec(Some(DxAccess(None,Some(AccessLevel.UPLOAD),None,None,None)), None, None, Some(DxTimeout(None, None, Some(30))))), None)
            )
        )
    }

    it should "include optional details in per task attributes" in {
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
               |      "details": {
               |        "upstreamProjects": [
               |          {
               |            "name": "GATK4",
               |            "repoUrl": "https://github.com/broadinstitute/gatk",
               |            "version": "GATK-4.0.1.2",
               |            "license": "BSD-3-Clause",
               |            "licenseUrl": "https://github.com/broadinstitute/LICENSE.TXT",
               |            "author": "Broad Institute"
               |          }
               |        ]
               |      }
               |    }
               |  }
               |}
               |""".stripMargin.parseJson

        val extras = Extras.parse(runSpec, verbose)
        extras.defaultTaskDxAttributes should be (
            Some(
                DxAttrs(
                    Some(DxRunSpec(None,None,None,Some(DxTimeout(None,Some(12),None)))),
                    None
                )))
        extras.perTaskDxAttributes should be (
            Map("Add" -> DxAttrs(
                    None,
                    Some(DxDetails(Some(
                                       List(DxLicense(
                                                "GATK4",
                                                "https://github.com/broadinstitute/gatk",
                                                "GATK-4.0.1.2",
                                                "BSD-3-Clause",
                                                "https://github.com/broadinstitute/LICENSE.TXT",
                                                "Broad Institute"))))))
            )
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
        extras.dockerRegistry should be (
            Some(DockerRegistry(
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

    it should "convert DxLicense to JsValue" in {

        val dxDetailsJson : JsValue =
            """|[
               |   {
               |      "author":"author1",
               |      "license":"license1",
               |      "licenseUrl":"licenseURL",
               |      "name":"name",
               |      "repoUrl":"repoURL",
               |      "version":"version1"
               |   },
               |   {
               |      "author":"author2",
               |      "license":"license2",
               |      "licenseUrl":"licenseURL",
               |      "name":"name2",
               |      "repoUrl":"repoURL",
               |      "version":"version2"
               |   }
               |]
               |""".stripMargin.parseJson


        val upstreamProjects  = List(
            DxLicense("name", "repoURL", "version1", "license1", "licenseURL", "author1"),
            DxLicense("name2", "repoURL", "version2", "license2", "licenseURL", "author2"),
            )

        val dxDetails = DxDetails(Some(upstreamProjects))

        val result = dxDetails.toDetailsJson
        result("upstreamProjects") should be (dxDetailsJson)
    }

    it should "all DxAttr to return RunSpec Json" in {

        val expectedPolicy = """
            |{
            |  "*": {
            |    "minutes": 30
            |  }
            |}
          """.stripMargin.parseJson

        val expected: Map[String, JsValue] = Map("timeoutPolicy" -> expectedPolicy)

        val dxAttrs: DxAttrs = DxAttrs(
            Some(DxRunSpec(
                     None, None, None, Some(DxTimeout(None, None, Some(30))))),
            None
        )

        val runSpecJson: Map[String, JsValue] = dxAttrs.getRunSpecJson
        (runSpecJson) should be (expected)

    }

    it should "all DxAttr to return empty runSpec and details Json" in {

        val dxAttrs = DxAttrs(None, None)

        val runSpecJson = dxAttrs.getRunSpecJson
        runSpecJson should be (Map.empty)

        val detailJson = dxAttrs.getDetailsJson
        detailJson should be (Map.empty)

    }

    it should "all DxAttr to return Details Json" in {

        val expectedContent =
            """
            |[
            |  {
            |    "name": "GATK4",
            |    "repoUrl": "https://github.com/broadinstitute/gatk",
            |    "version": "GATK-4.0.1.2",
            |    "license": "BSD-3-Clause",
            |    "licenseUrl": "https://github.com/broadinstitute/LICENSE.TXT",
            |    "author": "Broad Institute"
            |  }
            |]
            |
          """.stripMargin.parseJson

        val expected: Map[String, JsValue] = Map("upstreamProjects" -> expectedContent)

        val dxAttrs: DxAttrs = DxAttrs(
            None,
            Some(DxDetails(Some(
                               List(DxLicense(
                                        "GATK4",
                                        "https://github.com/broadinstitute/gatk",
                                        "GATK-4.0.1.2",
                                        "BSD-3-Clause",
                                        "https://github.com/broadinstitute/LICENSE.TXT",
                                        "Broad Institute")
                               )
                           ))
            )
        )

        val detailsJson = dxAttrs.getDetailsJson

        expected should be (detailsJson)
    }
}
