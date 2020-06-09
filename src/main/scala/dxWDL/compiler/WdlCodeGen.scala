package dxWDL.compiler

import scala.util.matching.Regex
import wdlTools.eval.WdlValues
import wdlTools.syntax.{CommentMap, WdlVersion}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import dxWDL.base.{Language, ParseWomSourceFile, Utils, Verbose}
import dxWDL.base.WomTypeSerialization.typeName
import wdlTools.types.WdlTypes.T_Task

case class WdlCodeGen(verbose: Verbose,
                      typeAliases: Map[String, WdlTypes.T],
                      language: Language.Value) {

  // A self contained WDL workflow
  private val wdlVersion: WdlVersion = {
    language match {
      case Language.WDLvDraft2 =>
        Utils.warning(verbose, "Upgrading draft-2 input to verion 1.0")
        WdlVersion.V1
      case Language.WDLv1_0 => WdlVersion.V1
      case Language.WDLv2_0 => WdlVersion.V2
      case other =>
        throw new Exception(s"Unsupported language version ${other}")
    }
  }

  private lazy val typeAliasDefinitions: Vector[TAT.StructDefinition] = {
    val sortedTypeAliases = SortTypeAliases(verbose).apply(typeAliases.toVector)
    sortedTypeAliases.map {
      case (name, wdlType: WdlTypes.T_Struct) =>
        TAT.StructDefinition(name, wdlType, wdlType.members, null)
    }
  }

  // create a wdl-value of a specific type.
  private def genDefaultValueOfType(wdlType: WdlTypes.T): TAT.Expr = {
    wdlType match {
      case WdlTypes.T_Boolean => TAT.ValueBoolean(value = true, wdlType, null)
      case WdlTypes.T_Int     => TAT.ValueInt(0, wdlType, null)
      case WdlTypes.T_Float   => TAT.ValueFloat(0.0, wdlType, null)
      case WdlTypes.T_String  => TAT.ValueString("", wdlType, null)
      case WdlTypes.T_File    => TAT.ValueString("dummy.txt", wdlType, null)

      // We could convert an optional to a null value, but that causes
      // problems for the pretty printer.
      // WdlValues.V_OptionalValue(wdlType, None)
      case WdlTypes.T_Optional(t) => genDefaultValueOfType(t)

      // The WdlValues.V_Map type HAS to appear before the array types, because
      // otherwise it is coerced into an array. The map has to
      // contain at least one key-value pair, otherwise you get a type error.
      case WdlTypes.T_Map(keyType, valueType) =>
        val k = genDefaultValueOfType(keyType)
        val v = genDefaultValueOfType(valueType)
        TAT.ExprMap(Map(k -> v), wdlType, null)

      // an empty array
      case WdlTypes.T_Array(_, false) =>
        TAT.ExprArray(Vector.empty, wdlType, null)

      // Non empty array
      case WdlTypes.T_Array(t, true) =>
        TAT.ExprArray(Vector(genDefaultValueOfType(t)), wdlType, null)

      case WdlTypes.T_Pair(lType, rType) =>
        TAT.ExprPair(genDefaultValueOfType(lType), genDefaultValueOfType(rType), wdlType, null)

      case WdlTypes.T_Struct(_, typeMap) =>
        val members = typeMap.map {
          case (fieldName, t) =>
            val key: TAT.Expr = TAT.ValueString(fieldName, WdlTypes.T_String, null)
            key -> genDefaultValueOfType(t)
        }
        TAT.ExprMap(members, wdlType, null)

      case _ => throw new Exception(s"Unhandled type ${wdlType}")
    }
  }

  private def wdlString(value: WdlValues.V): String = {
    value match {
      case WdlValues.V_Null           => "null"
      case WdlValues.V_Boolean(value) => value.toString
      case WdlValues.V_Int(value)     => value.toString
      case WdlValues.V_Float(value)   => value.toString
      case WdlValues.V_String(value)  => s""""${value}""""
      case WdlValues.V_File(value)    => s""""${value}""""

      // compound values
      case WdlValues.V_Pair(l, r) =>
        s"(${wdlString(l)} , ${wdlString(r)})"
      case WdlValues.V_Array(value) =>
        val elems = value.map(wdlString).mkString(",")
        s"[${elems}]"
      case WdlValues.V_Map(value) =>
        val m = value
          .map {
            case (k, v) =>
              s"${wdlString(k)} : ${wdlString(v)}"
          }
          .mkString(", ")
        s"""{${m}}"""

      case WdlValues.V_Optional(value) =>
        wdlString(value)
      case WdlValues.V_Struct(_, members) =>
        val membersStr = members.map {
          case (k, v) =>
            s"${k} : ${wdlString(v)}"
        }.toVector
        s"""object { ${membersStr.mkString(", ")} }"""

      case WdlValues.V_Object(members) =>
        val membersStr = members.map {
          case (k, v) =>
            s"${k} : ${wdlString(v)}"
        }.toVector
        s"""object { ${membersStr.mkString(", ")} }"""

      case other =>
        throw new Exception(s"Unhandled value ${other}")
    }
  }

  /*
Create a header for a task/workflow. This is an empty task
that includes the input and output definitions. It is used
to
(1) allow linking to native DNAx applets (and workflows in the future).
(2) make a WDL file stand-alone, without imports

For example, the stub for the Add task:
task Add {
    input {
      Int a
      Int b
    }
    command {
    command <<<
        python -c "print(${a} + ${b})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

is:
task Add {
   input {
     Int a
     Int b
   }
   command {}
   output {
       Int result
   }
  }
   */
  private def genTaskHeader(callable: IR.Callable): WdlCodeSnippet = {
    /*Utils.trace(verbose.on,
                    s"""|taskHeader  callable=${callable.name}
                        |  inputs= ${callable.inputVars.map(_.name)}
                        |  outputs= ${callable.outputVars.map(_.name)}"""
                        .stripMargin)*/

    // Sort the inputs by name, so the result will be deterministic.
    val inputs =
      callable.inputVars
        .sortWith(_.name < _.name)
        .map { cVar =>
          cVar.default match {
            case None =>
              s"    ${typeName(cVar.womType)} ${cVar.name}"
            case Some(wValue) =>
              s"    ${typeName(cVar.womType)} ${cVar.name} = ${wdlString(wValue)}"
          }
        }
        .mkString("\n")

    val outputs =
      callable.outputVars
        .sortWith(_.name < _.name)
        .map { cVar =>
          val defaultVal = genDefaultValueOfType(cVar.womType)
          s"    ${typeName(cVar.womType)} ${cVar.name} = ${wdlString(defaultVal)}"
        }
        .mkString("\n")

    language match {
      case Language.WDLvDraft2 =>
        // Draft-2 does not support the input block.
        WdlCodeSnippet(
            s"""|task ${callable.name} {
                |${inputs}
                |
                |  command {}
                |  output {
                |${outputs}
                |  }
                |}""".stripMargin
        )
      case Language.WDLv1_0 | Language.WDLv2_0 =>
        WdlCodeSnippet(
            s"""|task ${callable.name} {
                |  input {
                |${inputs}
                |  }
                |  command {}
                |  output {
                |${outputs}
                |  }
                |}""".stripMargin
        )
      case other =>
        throw new Exception(s"Unsupported language version ${other}")
    }
  }

  /**
    * Generate a WDL stub fore a DNAnexus applet.
    * @param id the applet ID
    * @param appletName the applet name
    * @param inputSpec the applet inputs
    * @param outputSpec the applet outputs
    * @return an AST.Task
    */
  def genDnanexusAppletStub(id: String,
                            appletName: String,
                            inputSpec: Map[String, WdlTypes.T],
                            outputSpec: Map[String, WdlTypes.T]): TAT.Task = {

    val meta = TAT.MetaSection(
        Map(
            "type" -> TAT.MetaValueString("native", null),
            "id" -> TAT.MetaValueString(id, null)
        ),
        null
    )
    TAT.Task(
        appletName,
        T_Task(appletName, inputSpec.map {
          case (name, wdlType) => name -> (wdlType, false)
        }, outputSpec),
        inputSpec.map {
          case (name, wdlType) => TAT.RequiredInputDefinition(name, wdlType, null)
        }.toVector,
        outputSpec.map {
          case (name, wdlType) =>
            val expr = genDefaultValueOfType(wdlType)
            TAT.OutputDefinition(name, wdlType, expr, null)
        }.toVector,
        TAT.CommandSection(Vector.empty, null),
        Vector.empty,
        Some(meta),
        parameterMeta = None,
        runtime = None,
        hints = None,
        text = null
    )
  }

  def standAloneTask(task: TAT.Task): TAT.Document = {
    TAT.Document(
        None,
        null,
        TAT.Version(wdlVersion, null),
        typeAliasDefinitions :+ task,
        None,
        null,
        CommentMap.empty
    )
  }

  // A workflow must have definitions for all the tasks it
  // calls. However, a scatter calls tasks that are missing from
  // the WDL file we generate. To ameliorate this, we add stubs for
  // called tasks. The generated tasks are named by their
  // unqualified names, not their fully-qualified names. This works
  // because the WDL workflow must be "flattenable".
  def standAloneWorkflow(wf: TAT.Workflow, allCalls: Vector[IR.Callable]): String = {
    val taskStubs: Map[String, String] =
      allCalls.foldLeft(Map.empty[String, String]) {
        case (accu, callable) =>
          if (accu contains callable.name) {
            // we have already created a stub for this call
            accu
          } else {
            val sourceCode = callable match {
              case IR.Applet(_, _, _, _, _, IR.AppletKindTask(_), taskSourceCode, _, _) =>
                // This is a task, include its source code, instead of a header.
                val taskDir = ParseWomSourceFile(false).scanForTasks(taskSourceCode)
                assert(taskDir.size == 1)
                val taskBody = taskDir.values.head
                WdlCodeSnippet(taskBody)

              case _ =>
                // no existing stub, create it
                genTaskHeader(callable)
            }
            accu + (callable.name -> sourceCode)
          }
      }

    // sort the task order by name, so the generated code will be deterministic
    val tasksStr = taskStubs.toVector
      .sortWith(_._1 < _._1)
      .map { case (_, wdlCode) => wdlCode.value }
      .mkString("\n\n")
    val wfWithoutImportCalls = flattenWorkflow(originalWorkflowSource)
    val wdlWfSource =
      List(versionString() + "\n",
           "# struct definitions",
           typeAliasDefinitions,
           "# Task headers",
           tasksStr,
           "# Workflow with imports made local",
           wfWithoutImportCalls).mkString("\n")

    TAT.Document(
        None,
        null,
        TAT.Version(wdlVersion, null),
        typeAliasDefinitions ++ taskStubs,
        wfWithoutImportCalls,
        null,
        CommentMap.empty
    )
  }

  // A workflow can import other libraries:
  //
  // import "library.wdl" as lib
  // workflow foo {
  //   call lib.Multiply as mul { ... }
  //   call lib.Add { ... }
  //   call lib.Nice as nice { ... }
  //   call lib.Hello
  // }
  //
  // rewrite the workflow, and remove the calls to external libraries.
  //
  // workflow foo {
  //   call Multiply as mul { ... }
  //   call Add { ... }
  //   call Nice as nice { ... }
  //   call Nice as nice { ... }
  //   call Hello
  // }
  //
  private val callLibrary: Regex = "^(\\s*)call(\\s+)(\\w+)\\.(\\w+)(\\s+)(.+)".r
  private val callLibraryNoArgs: Regex = "^(\\s*)call(\\s+)(\\w+)\\.(\\w+)(\\s*)".r
  private def flattenWorkflow(wdlWfSource: String): String = {
    val originalLines = wdlWfSource.split("\n").toList
    val cleanLines = originalLines.map { line =>
      val allMatches = callLibrary.findAllMatchIn(line).toList
      assert(allMatches.size <= 1)
      val newLine =
        if (allMatches.isEmpty) {
          line
        } else {
          val m = allMatches.head
          val callee: String = m.group(4)
          val rest = m.group(6)
          s"call ${callee} ${rest}"
        }

      // call with no arguments
      val allMatches2 = callLibraryNoArgs.findAllMatchIn(newLine).toList
      assert(allMatches2.size <= 1)
      if (allMatches2.isEmpty) {
        newLine
      } else {
        val m = allMatches2.head
        val callee: String = m.group(4)
        s"call ${callee}"
      }
    }
    cleanLines.mkString("\n")
  }

}
