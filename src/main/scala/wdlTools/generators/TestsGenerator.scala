package wdlTools.generators

import java.net.URL
import scala.collection.mutable

import wdlTools.syntax.AbstractSyntax._

import spray.json._

case class TestsGenerator(generatedFiles: mutable.Map[URL, String] = mutable.HashMap.empty) {
  def apply(url: URL, wdlName: String, doc: Document): Unit = {
    val data: mutable.Map[String, JsValue] = mutable.HashMap.empty

    def getDummyValue(value: Type): JsValue = {
      value match {
        case _: TypeString      => JsString("foo")
        case _: TypeFile        => JsObject(Map("url" -> JsString("http://url/of/data/file")))
        case _: TypeInt         => JsNumber(0)
        case _: TypeFloat       => JsNumber(1.0)
        case _: TypeBoolean     => JsBoolean(true)
        case TypeArray(t, _, _) => JsArray(getDummyValue(t))
        case TypeMap(k, v, _)   => JsObject(k.toString -> getDummyValue(v))
        case _: TypeObject      => JsObject("foo" -> JsString("bar"))
        case TypePair(l, r, _)  => JsObject("left" -> getDummyValue(l), "right" -> getDummyValue(r))
        case TypeStruct(_, members, _) =>
          JsObject(members.collect {
            case StructMember(name, dataType, _) if !dataType.isInstanceOf[TypeOptional] =>
              name -> getDummyValue(dataType)
          }.toMap)
        case other => throw new Exception(s"Unrecognized type ${other}")
      }
    }

    def addData(declarations: Vector[Declaration]): JsObject = {
      JsObject(declarations.collect {
        case Declaration(name, wdlType, expr, _)
            if expr.isEmpty && !wdlType.isInstanceOf[TypeOptional] && !data.contains(name) =>
          val dataName = s"input_${name}"
          data(dataName) = getDummyValue(wdlType)
          name -> JsString(dataName)
      }.toMap)
    }

    def addInputs(inputs: Option[InputSection]): JsObject = {
      if (inputs.isDefined) {
        addData(inputs.get.declarations)
      } else {
        JsObject()
      }
    }

    def addOutputs(outputs: Option[OutputSection]): JsObject = {
      if (outputs.isDefined) {
        addData(outputs.get.declarations)
      } else {
        JsObject()
      }
    }

    def createTest(name: String,
                   wdl: String,
                   inputs: JsObject,
                   expected: JsObject,
                   taskName: Option[String] = None): JsObject = {
      val fields: Map[String, JsValue] = Map(
          "name" -> JsString(name),
          "wdl" -> JsString(wdl),
          "inputs" -> inputs,
          "expected" -> expected
      )
      val taskNameField = if (taskName.isDefined) {
        Map("task_name" -> JsString(taskName.get))
      } else {
        Map.empty
      }
      JsObject(fields ++ taskNameField)
    }

    val workflowTest: Vector[JsObject] = Vector(doc.workflow.map { workflow =>
      createTest(
          name = s"test_workflow_${workflow.name}",
          wdlName,
          addInputs(workflow.input),
          addOutputs(workflow.output)
      )
    }).flatten

    val taskTests: Vector[JsObject] = doc.elements.collect {
      case task: Task =>
        createTest(
            name = s"test_task_${task.name}",
            wdlName,
            addInputs(task.input),
            addOutputs(task.output),
            Some(task.name)
        )
    }

    val json = JsObject(
        "data" -> JsObject(data.toMap),
        "tests" -> JsArray(workflowTest ++ taskTests)
    ).prettyPrint

    generatedFiles(url) = json
  }
}
