package dx.compiler

import dx.core.languages.Language
import wdlTools.eval.WdlValues
import wdlTools.generators.code.WdlV1Generator
import wdlTools.syntax.{CommentMap, SourceLocation, WdlVersion}
import wdlTools.types.WdlTypes.T_Task
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{Logger, StringFileSource}

case class WdlCodeGen(logger: Logger,
                      typeAliases: Map[String, WdlTypes.T],
                      language: Language.Value) {

  private val locPlaceholder: SourceLocation = SourceLocation.empty

  // A self contained WDL workflow
  val wdlVersion: WdlVersion = {
    language match {
      case Language.WDLvDraft2 =>
        logger.warning("Upgrading draft-2 input to verion 1.0")
        WdlVersion.V1
      case Language.WDLv1_0 => WdlVersion.V1
      case Language.WDLv2_0 => WdlVersion.V2
      case other =>
        throw new Exception(s"Unsupported language version ${other}")
    }
  }

  private lazy val typeAliasDefinitions: Vector[TAT.StructDefinition] = {
    val sortedTypeAliases = SortTypeAliases(logger).apply(typeAliases.toVector)
    sortedTypeAliases.map {
      case (name, wdlType: WdlTypes.T_Struct) =>
        TAT.StructDefinition(name, wdlType, wdlType.members, locPlaceholder)
      case other => throw new RuntimeException(s"Unexpected type alias ${other}")
    }
  }

  // create a wdl-value of a specific type.
  private[compiler] def genDefaultValueOfType(wdlType: WdlTypes.T): TAT.Expr = {
    wdlType match {
      case WdlTypes.T_Boolean => TAT.ValueBoolean(value = true, wdlType, locPlaceholder)
      case WdlTypes.T_Int     => TAT.ValueInt(0, wdlType, locPlaceholder)
      case WdlTypes.T_Float   => TAT.ValueFloat(0.0, wdlType, locPlaceholder)
      case WdlTypes.T_String  => TAT.ValueString("", wdlType, locPlaceholder)
      case WdlTypes.T_File    => TAT.ValueString("placeholder.txt", wdlType, locPlaceholder)

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
        TAT.ExprMap(Map(k -> v), wdlType, locPlaceholder)

      // an empty array
      case WdlTypes.T_Array(_, false) =>
        TAT.ExprArray(Vector.empty, wdlType, locPlaceholder)

      // Non empty array
      case WdlTypes.T_Array(t, true) =>
        TAT.ExprArray(Vector(genDefaultValueOfType(t)), wdlType, locPlaceholder)

      case WdlTypes.T_Pair(lType, rType) =>
        TAT.ExprPair(genDefaultValueOfType(lType),
                     genDefaultValueOfType(rType),
                     wdlType,
                     locPlaceholder)

      case WdlTypes.T_Struct(_, typeMap) =>
        val members = typeMap.map {
          case (fieldName, t) =>
            val key: TAT.Expr = TAT.ValueString(fieldName, WdlTypes.T_String, locPlaceholder)
            key -> genDefaultValueOfType(t)
        }
        TAT.ExprObject(members, wdlType, locPlaceholder)

      case _ => throw new Exception(s"Unhandled type ${wdlType}")
    }
  }

  private[compiler] def wdlValueToExpr(value: WdlValues.V): TAT.Expr = {
    def seqToType(vec: Iterable[TAT.Expr]): WdlTypes.T = {
      vec.headOption.map(_.wdlType).getOrElse(WdlTypes.T_Any)
    }

    value match {
      case WdlValues.V_Null => TAT.ValueNull(WdlTypes.T_Any, locPlaceholder)
      case WdlValues.V_Boolean(value) =>
        TAT.ValueBoolean(value, WdlTypes.T_Boolean, locPlaceholder)
      case WdlValues.V_Int(value)   => TAT.ValueInt(value, WdlTypes.T_Int, locPlaceholder)
      case WdlValues.V_Float(value) => TAT.ValueFloat(value, WdlTypes.T_Float, locPlaceholder)
      case WdlValues.V_String(value) =>
        TAT.ValueString(value, WdlTypes.T_String, locPlaceholder)
      case WdlValues.V_File(value) => TAT.ValueFile(value, WdlTypes.T_File, locPlaceholder)
      case WdlValues.V_Directory(value) =>
        TAT.ValueDirectory(value, WdlTypes.T_Directory, locPlaceholder)

      // compound values
      case WdlValues.V_Pair(l, r) =>
        val lExpr = wdlValueToExpr(l)
        val rExpr = wdlValueToExpr(r)
        TAT.ExprPair(lExpr, rExpr, WdlTypes.T_Pair(lExpr.wdlType, rExpr.wdlType), locPlaceholder)
      case WdlValues.V_Array(value) =>
        val valueExprs = value.map(wdlValueToExpr)
        TAT.ExprArray(valueExprs, seqToType(valueExprs), locPlaceholder)
      case WdlValues.V_Map(value) =>
        val keyExprs = value.keys.map(wdlValueToExpr)
        val valueExprs = value.values.map(wdlValueToExpr)
        TAT.ExprMap(keyExprs.zip(valueExprs).toMap,
                    WdlTypes.T_Map(seqToType(keyExprs), seqToType(valueExprs)),
                    locPlaceholder)

      case WdlValues.V_Optional(value) => wdlValueToExpr(value)
      case WdlValues.V_Struct(name, members) =>
        val memberExprs: Map[TAT.Expr, TAT.Expr] = members.map {
          case (name, value) =>
            TAT.ValueString(name, WdlTypes.T_String, locPlaceholder) -> wdlValueToExpr(value)
          case other => throw new RuntimeException(s"Unexpected member ${other}")
        }
        val memberTypes = memberExprs.map {
          case (name: TAT.ValueString, value) => name.value -> value.wdlType
          case other                          => throw new RuntimeException(s"Unexpected member ${other}")
        }
        TAT.ExprMap(memberExprs, WdlTypes.T_Struct(name, memberTypes), locPlaceholder)

      case WdlValues.V_Object(members) =>
        val memberExprs = members.map {
          case (name, value) =>
            val key: TAT.Expr = TAT.ValueString(name, WdlTypes.T_String, locPlaceholder)
            key -> wdlValueToExpr(value)
        }
        TAT.ExprObject(memberExprs, WdlTypes.T_Object, locPlaceholder)

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
  private def genTaskHeader(callable: IR.Callable): TAT.Task = {
    /*Utils.trace(verbose.on,
                    s"""|taskHeader  callable=${callable.name}
                        |  inputs= ${callable.inputVars.map(_.name)}
                        |  outputs= ${callable.outputVars.map(_.name)}"""
                        .stripMargin)*/

    // Sort the inputs by name, so the result will be deterministic.
    val inputs: Vector[TAT.InputDefinition] =
      callable.inputVars
        .sortWith(_.name < _.name)
        .map { cVar =>
          cVar.default match {
            case None =>
              TAT.RequiredInputDefinition(cVar.name, cVar.wdlType, locPlaceholder)
            case Some(wValue) =>
              TAT.OverridableInputDefinitionWithDefault(cVar.name,
                                                        cVar.wdlType,
                                                        wdlValueToExpr(wValue),
                                                        locPlaceholder)
          }
        }

    val outputs: Vector[TAT.OutputDefinition] =
      callable.outputVars
        .sortWith(_.name < _.name)
        .map { cVar =>
          val defaultVal = genDefaultValueOfType(cVar.wdlType)
          TAT.OutputDefinition(cVar.name, cVar.wdlType, defaultVal, locPlaceholder)
        }

    language match {
      case Language.WDLvDraft2 | Language.WDLv1_0 | Language.WDLv2_0 =>
        TAT.Task(
            callable.name,
            WdlTypes.T_Task(
                callable.name,
                inputs.map {
                  case TAT.RequiredInputDefinition(name, wdlType, _) =>
                    name -> (wdlType, false)
                  case other: TAT.InputDefinition =>
                    other.name -> (other.wdlType, true)
                }.toMap,
                outputs.map(d => d.name -> d.wdlType).toMap
            ),
            inputs,
            outputs,
            TAT.CommandSection(Vector.empty, locPlaceholder),
            Vector.empty,
            None,
            None,
            None,
            None,
            locPlaceholder
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
            "type" -> TAT.MetaValueString("native", locPlaceholder),
            "id" -> TAT.MetaValueString(id, locPlaceholder)
        ),
        locPlaceholder
    )
    TAT.Task(
        appletName,
        T_Task(appletName, inputSpec.map {
          case (name, wdlType) => name -> (wdlType, false)
        }, outputSpec),
        inputSpec.map {
          case (name, wdlType) => TAT.RequiredInputDefinition(name, wdlType, locPlaceholder)
        }.toVector,
        outputSpec.map {
          case (name, wdlType) =>
            val expr = genDefaultValueOfType(wdlType)
            TAT.OutputDefinition(name, wdlType, expr, locPlaceholder)
        }.toVector,
        TAT.CommandSection(Vector.empty, locPlaceholder),
        Vector.empty,
        Some(meta),
        parameterMeta = None,
        runtime = None,
        hints = None,
        loc = locPlaceholder
    )
  }

  def standAloneTask(task: TAT.Task): TAT.Document = {
    TAT.Document(
        StringFileSource.empty,
        TAT.Version(wdlVersion, locPlaceholder),
        typeAliasDefinitions :+ task,
        None,
        locPlaceholder,
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
  //   call Hello
  // }
  private def cleanCalls(body: Vector[TAT.WorkflowElement]): Vector[TAT.WorkflowElement] = {
    body.map {
      case call: TAT.Call =>
        call.copy(fullyQualifiedName = call.unqualifiedName)
      case scat: TAT.Scatter =>
        scat.copy(body = cleanCalls(scat.body))
      case cond: TAT.Conditional =>
        cond.copy(body = cleanCalls(cond.body))
      case other => other
    }
  }

  // A workflow must have definitions for all the tasks it
  // calls. However, a scatter calls tasks that are missing from
  // the WDL file we generate. To ameliorate this, we add stubs for
  // called tasks. The generated tasks are named by their
  // unqualified names, not their fully-qualified names. This works
  // because the WDL workflow must be "flattenable".
  def standAloneWorkflow(wf: TAT.Workflow, allCalls: Vector[IR.Callable]): TAT.Document = {
    val tasks: Vector[TAT.Task] =
      allCalls
        .foldLeft(Map.empty[String, TAT.Task]) {
          case (accu, callable) =>
            if (accu contains callable.name) {
              // we have already created a stub for this call
              accu
            } else {
              val stub: TAT.Task = callable match {
                case IR.Applet(_, _, _, _, _, IR.AppletKindTask(_), doc, _, _) =>
                  // This is a task, include its source instead of a header.
                  val tasks = doc.elements.collect {
                    case t: TAT.Task => t
                  }
                  assert(tasks.size == 1)
                  tasks.head
                case _ =>
                  // no existing stub, create it
                  genTaskHeader(callable)
              }
              accu + (callable.name -> stub)
            }
        }
        // sort the task order by name, so the generated code will be deterministic
        .toVector
        .sortWith(_._1 < _._1)
        .map { case (_, task) => task }

    val wfWithoutImportCalls = wf.copy(body = cleanCalls(wf.body))

    TAT.Document(
        StringFileSource.empty,
        TAT.Version(wdlVersion, locPlaceholder),
        typeAliasDefinitions ++ tasks,
        Some(wfWithoutImportCalls),
        locPlaceholder,
        CommentMap.empty
    )
  }

  def generateDocument(doc: TAT.Document): String = {
    val generator = WdlV1Generator()
    generator.generateDocument(doc).mkString("\n")
  }
}
