package dxWDL.dx

import com.dnanexus.DXAPI
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import spray.json._
import DefaultJsonProtocol._
import com.fasterxml.jackson.databind.JsonNode


class DxWorkflowTest extends FlatSpec with BeforeAndAfterEach with Matchers {

  val TEST_PROJECT = "dxWDL_playground"
  lazy val dxTestProject: DxProject =
    try {
      DxPath.resolveProject(TEST_PROJECT)
    } catch {
      case e: Exception =>
        throw new Exception(
          s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
              |the platform on staging.""".stripMargin
        )
    }
  lazy val username = System.getProperty("user.name")
  lazy val unitTestsPath = s"/unit_tests/${username}"
  dxTestProject.newFolder(unitTestsPath, parents = true)

  var testWorkflow: DxWorkflow = _

  override def beforeEach(): Unit = {

    val req = Map(
      "project" -> dxTestProject.id,
      "folder" -> unitTestsPath
    ).toJson.asJsObject

    val workflowJSON = DXAPI.workflowNew(
      DxUtils.jsonNodeOfJsValue(req) , classOf[JsonNode]).toString().parseJson

    val workflowID = workflowJSON.asJsObject().fields.get("id") match {
      case Some(JsString(x)) => x
      case _ => throw new Exception("Unable to setup workflow for DxWorkflowTest")
    }
    testWorkflow = DxWorkflow(workflowID, Some(dxTestProject))
  }

  override def afterEach(): Unit = {
    dxTestProject.removeObjects(Vector(testWorkflow))
  }

  it should "Be able to set details of a workflow" in {

    val mockDetails =
      """
        |{
        |  "test": "a"
        |}
        |""".stripMargin.parseJson

    testWorkflow.setDetails(mockDetails)

    val wfDescribe = testWorkflow.describe(Set(Field.Details))
    wfDescribe.details.get shouldBe mockDetails
  }

  it should "Be able to update details of a workflow" in {

    val mockDetails = JsObject(
        "test_a" -> JsString("a"), "test_b" -> JsString("b")
    );

    testWorkflow.setDetails(mockDetails)
    val wfDescribe = testWorkflow.describe(Set(Field.Details))
    wfDescribe.details.get shouldBe mockDetails

    // update details
    val updates = JsObject("test_a" -> JsString("a2"), "test_c" -> JsString("c"))
    val updatedMockDetails = JsObject(mockDetails.fields ++ updates.fields)
    testWorkflow.updateDetails(updatedMockDetails)
    val updatedWfDescribe = testWorkflow.describe(Set(Field.Details))
    updatedWfDescribe.details.get shouldBe updatedMockDetails

    updatedWfDescribe.details match {
      case Some(x: JsValue) => x.asJsObject.fields.get("test_a") match {
        case Some(JsString(x)) => x shouldBe "a2"
        case _ => throw new Exception("Workflow not updated!")
      }
      case _ => throw new Exception("Workflow not updated!")
    }
  }

}
