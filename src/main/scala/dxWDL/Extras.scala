package dxWDL

// Place to put any extra options, equivalent to Cromwell workflowOptions.
// Also, allows dnanexus specific configuration per task.

import com.dnanexus.AccessLevel
import spray.json._
import wdl.draft2.model.WdlExpression
import wom.values._

case class DxExecPolicy(restartOn: Option[Map[String, Int]],
                        maxRestarts: Option[Int]) {
    def toJson : Map[String, JsValue] = {
        val restartOnFields = restartOn match {
            case None => Map.empty
            case Some(m) =>
                val jsm = m.map{ case (name, i) => name -> JsNumber(i) }
                Map("restartOn" -> JsObject(jsm))
        }
        val maxRestartsFields = maxRestarts match {
            case None => Map.empty
            case Some(n) => Map("maxRestarts" -> JsNumber(n))
        }
        val m = restartOnFields ++ maxRestartsFields
        if (m.isEmpty)
            Map.empty
        else
            Map("executionPolicy" -> JsObject(m))
    }
}

case class DxTimeout(days: Option[Int],
                     hours: Option[Int],
                     minutes: Option[Int]) {
    def toJson : Map[String, JsValue] = {
        val daysField : Map[String, JsValue] = days match {
            case None => Map.empty
            case Some(d) => Map("days" -> JsNumber(d))
        }
        val hoursField : Map[String, JsValue] = hours match {
            case None => Map.empty
            case Some(d) => Map("hours" -> JsNumber(d))
        }
        val minutesField : Map[String, JsValue] = minutes match {
            case None => Map.empty
            case Some(d) => Map("minutes" -> JsNumber(d))
        }
        val timeoutFields = daysField ++ hoursField ++ minutesField
        if (timeoutFields.isEmpty)
            Map.empty
        else
            Map("timeoutPolicy" -> JsObject("*" -> JsObject(timeoutFields)))
    }
}
case class DxAccess(network: Option[Vector[String]],
                    project: Option[AccessLevel],
                    allProjects: Option[AccessLevel],
                    developer: Option[Boolean],
                    projectCreation: Option[Boolean]) {
    def toJson : Map[String, JsValue] = {
        val networkField: Map[String, JsValue] = network match {
            case None => Map.empty
            case Some(hostsAndNetworks) =>
                val arr = hostsAndNetworks.map{ x => JsString(x) }.toVector
                Map("network" -> JsArray(arr))
        }
        val projectField : Map[String, JsValue] = project match {
            case None => Map.empty
            case Some(x) => Map("project" -> JsString(x.toString))
        }
        val allProjectsField: Map[String, JsValue] = allProjects match {
            case None => Map.empty
            case Some(x) => Map("allProjects" -> JsString(x.toString))
        }
        val developerField: Map[String, JsValue] = developer match {
            case None => Map.empty
            case Some(x) => Map("developer" -> JsBoolean(x))
        }
        val projectCreationField: Map[String, JsValue] = projectCreation match {
            case None => Map.empty
            case Some(x) => Map("projectCreation" -> JsBoolean(x))
        }
        networkField ++ projectField ++ allProjectsField ++ developerField ++ projectCreationField
    }

    private def takeMaxAccessLevel(x: AccessLevel,
                                   y: AccessLevel) : AccessLevel = {
        def levelToInt(al: AccessLevel) : Int = al match {
            case AccessLevel.ADMINISTER => 5
            case AccessLevel.CONTRIBUTE => 4
            case AccessLevel.UPLOAD => 3
            case AccessLevel.VIEW => 2
            case AccessLevel.NONE => 1
        }
        def intToLevel(i: Int) : AccessLevel = i match {
            case 5 => AccessLevel.ADMINISTER
            case 4 => AccessLevel.CONTRIBUTE
            case 3 => AccessLevel.UPLOAD
            case 2 => AccessLevel.VIEW
            case 1 => AccessLevel.NONE
            case _ => throw new Exception(s"sanity, bad integer level ${i}")
        }
        val xi = levelToInt(x)
        val yi = levelToInt(y)
        val mi = Math.max(xi, yi)
        intToLevel(mi)
    }

    // Merge with an additional set of access requirments.
    // Take the maximum for each field, and merge the network.
    def merge(dxa: DxAccess) : DxAccess = {
        def mergeAccessLevels(al1: Option[AccessLevel],
                              al2: Option[AccessLevel]) : Option[AccessLevel] = {
            Vector(al1, al2).flatten match {
                case Vector(x) => Some(x)
                case Vector(x, y) => Some(takeMaxAccessLevel(x, y))
                case _ => None
            }
        }
        def mergeBooleans(a: Option[Boolean], b: Option[Boolean]) : Option[Boolean] = {
            Vector(a, b).flatten match {
                case Vector(x) => Some(x)
                case Vector(x, y) => Some(x || y)
                case _ => None
            }
        }

        val networkMrg = Vector(network, dxa.network).flatten match {
            case Vector(x) => Some(x)
            case Vector(x, y) =>  Some((x.toSet ++ y.toSet).toVector)
            case _ => None
        }
        val projectMrg = mergeAccessLevels(project, dxa.project)
        val allProjectsMrg = mergeAccessLevels(allProjects, dxa.allProjects)
        val developerMrg = mergeBooleans(developer, dxa.developer)
        val projectCreationMrg = mergeBooleans(projectCreation, dxa.projectCreation)

        DxAccess(networkMrg, projectMrg, allProjectsMrg, developerMrg, projectCreationMrg)
    }
}

object DxAccess {
    val empty = DxAccess(None, None, None, None, None)
}

case class DxRunSpec(execPolicy: Option[DxExecPolicy],
                     timeoutPolicy: Option[DxTimeout],
                     access: Option[DxAccess]) {
    // The access field DOES NOT go into the run-specification in the API
    // call. It does fit, however, in dxapp.json.
    def toRunSpecJson : Map[String, JsValue] = {
        val execPolicyFields = execPolicy match {
            case None => Map.empty
            case Some(execPolicy) =>
                execPolicy.toJson
        }
        val timeoutFields = timeoutPolicy match {
            case None => Map.empty
            case Some(execPolicy) => execPolicy.toJson
        }
        execPolicyFields ++ timeoutFields
    }
}

case class Extras(defaultRuntimeAttributes: Map[String, WdlExpression],
                  defaultTaskDxAttributes: Option[DxRunSpec])

object Extras {
    val DX_INSTANCE_TYPE_ATTR = "dx_instance_type"
    val EXTRA_ATTRS = Set("default_runtime_attributes", "default_task_dx_attributes")
    val RUNTIME_ATTRS = Set(DX_INSTANCE_TYPE_ATTR, "memory", "disks", "cpu", "docker")
    val TASK_DX_ATTRS = Set("runSpec")
    val RUN_SPEC_ATTRS = Set("executionPolicy", "timeoutPolicy", "access")
    val RUN_SPEC_ACCESS_ATTRS = Set("network", "project", "allProjects", "developer", "projectCreation")
    val RUN_SPEC_TIMEOUT_ATTRS = Set("days", "hours", "minutes")
    val RUN_SPEC_EXEC_POLICY_ATTRS = Set("restartOn", "maxRestarts")
    val RUN_SPEC_EXEC_POLICY_RESTART_ON_ATTRS = Set("ExecutionError", "UnresponsiveWorker",
                                                    "JMInternalError", "AppInternalError",
                                                    "JobTimeoutExceeded", "*")

    private def wdlExpressionFromJsValue(jsv: JsValue) : WdlExpression = {
        val wValue: WomValue = jsv match {
            case JsBoolean(b) => WomBoolean(b.booleanValue)
            case JsNumber(nmb) => WomInteger(nmb.intValue)
            case JsString(s) => WomString(s)
            case other => throw new Exception(s"Unsupported json value ${other}")
        }
        WdlExpression.fromString(wValue.toWomString)
    }


    private def checkedParseIntField(fields: Map[String, JsValue],
                                     fieldName: String) : Option[Int] = {
        fields.get(fieldName) match {
            case None => None
            case Some(JsNumber(bnm)) => Some(bnm.intValue)
            case Some(other) => throw new Exception(s"Malformed ${fieldName} (${other})")
        }
    }

    private def checkedParseStringField(fields: Map[String, JsValue],
                                        fieldName: String) : Option[String] = {
        fields.get(fieldName) match {
            case None => None
            case Some(JsString(str)) => Some(str)
            case Some(other) => throw new Exception(s"Malformed ${fieldName} (${other})")
        }
    }

    private def checkedParseStringArrayField(fields: Map[String, JsValue],
                                             fieldName: String) : Option[Vector[String]] = {
        fields.get(fieldName) match {
            case None => None
            case Some(JsArray(vec)) =>
                val v = vec.map{
                    case JsString(str) => str
                    case other => throw new Exception(s"Malformed ${fieldName} (${other})")
                }
                Some(v)
            case Some(other) => throw new Exception(s"Malformed ${fieldName} (${other})")
        }
    }

    private def checkedParseBooleanField(fields: Map[String, JsValue],
                                        fieldName: String) : Option[Boolean] = {
        fields.get(fieldName) match {
            case None => None
            case Some(JsBoolean(b)) => Some(b)
            case Some(other) => throw new Exception(s"Malformed ${fieldName} (${other})")
        }
    }

    private def checkedParseAccessLevelField(fields: Map[String, JsValue],
                                             fieldName: String) : Option[AccessLevel] = {
        checkedParseStringField(fields, fieldName) match {
            case None =>
                return None
            case Some(s) =>
                for (x <-  AccessLevel.values()) {
                    if (x.toString.toLowerCase == s.toLowerCase)
                        return Some(x)
                }
                throw new Exception(s"Invalid access level ${s}")
        }
    }

    private def checkedParseObjectField(fields: Map[String, JsValue],
                                        fieldName: String) : JsValue = {
        fields.get(fieldName) match {
            case None => JsNull
            case Some(JsObject(fields)) => JsObject(fields)
            case Some(other) => throw new Exception(s"Malformed ${fieldName} (${other})")
        }
    }

    private def checkedParseMapStringInt(fields: Map[String, JsValue],
                                         fieldName: String) : Option[Map[String, Int]] = {
        val m = checkedParseObjectField(fields, fieldName)
        if (m == JsNull)
            return None
        val m1 = m.asJsObject.fields.map{
            case (name, JsNumber(nmb)) => name -> nmb.intValue
            case (name, other) => throw new Exception(s"Malformed ${fieldName} (${name} -> ${other})")
        }.toMap
        return Some(m1)
    }

    private def parseRuntimeAttrs(jsv: JsValue,
                                  verbose: Verbose) : Map[String, WdlExpression] = {
        if (jsv == JsNull)
            return Map.empty
        val fields = jsv.asJsObject.fields
        for (k <- fields.keys) {
            if (!(RUNTIME_ATTRS contains k))
                Utils.warning(verbose, s"""|Unsupported runtime attribute ${k},
                                           |we currently support ${RUNTIME_ATTRS}
                                           |""".stripMargin.replaceAll("\n", ""))
        }
        val attrs = fields
            .filter{ case (key,_) => RUNTIME_ATTRS contains key }
            .map{ case (name, jsValue) =>
                name -> wdlExpressionFromJsValue(jsValue) }.toMap
        return attrs
    }

    private def parseExecutionPolicy(jsv: JsValue) : Option[DxExecPolicy] = {
        if (jsv == JsNull)
            return None
        val fields = jsv.asJsObject.fields
        for (k <- fields.keys) {
            if (!(RUN_SPEC_EXEC_POLICY_ATTRS contains k))
                throw new Exception(s"""|Unsupported runSpec.access attribute ${k},
                                        |we currently support ${RUN_SPEC_EXEC_POLICY_ATTRS}
                                        |""".stripMargin.replaceAll("\n", ""))

        }

        val restartOn = checkedParseMapStringInt(fields, "restartOn")
        restartOn match {
            case None => ()
            case Some(restartOnPolicy) =>
                for (k <- restartOnPolicy.keys)
                    if (!(RUN_SPEC_EXEC_POLICY_RESTART_ON_ATTRS contains k))
                        throw new Exception(s"unknown field ${k} in restart policy")
        }

        val maxRestarts = checkedParseIntField(fields, "maxRestarts")
        return Some(DxExecPolicy(restartOn, maxRestarts))
    }

    private def parseAccess(jsv: JsValue) : Option[DxAccess] = {
        if (jsv == JsNull)
            return None
        val fields = jsv.asJsObject.fields
        for (k <- fields.keys) {
            if (!(RUN_SPEC_ACCESS_ATTRS contains k))
                throw new Exception(s"""|Unsupported runSpec.access attribute ${k},
                                        |we currently support ${RUN_SPEC_ACCESS_ATTRS}
                                        |""".stripMargin.replaceAll("\n", ""))

        }

        return Some(DxAccess(checkedParseStringArrayField(fields, "network"),
                             checkedParseAccessLevelField(fields, "project"),
                             checkedParseAccessLevelField(fields, "allProjects"),
                             checkedParseBooleanField(fields, "developer"),
                             checkedParseBooleanField(fields, "projectCreation")))
    }

    /* Only timeouts of the following format are supported:
         "timeoutPolicy": {
             "*": {
                 "hours": 12
             }
         },
     */
    private def parseTimeoutPolicy(jsv: JsValue) : Option[DxTimeout] = {
        if (jsv == JsNull)
            return None
        val fields = jsv.asJsObject.fields
        if (fields.keys.size != 1)
            throw new Exception("Exactly one entry-point timeout can be specified")
        val key = fields.keys.head
        if (key != "*")
            throw new Exception("""Only a general timeout for all entry points is supported ("*")""")
        val subObj = checkedParseObjectField(fields, "*")
        if (subObj == JsNull)
            return None
        val subFields = subObj.asJsObject.fields
        for (k <- subFields.keys) {
            if (!(RUN_SPEC_TIMEOUT_ATTRS contains k))
                throw new Exception(s"""|Unsupported runSpec.timeoutPolicy attribute ${k},
                                        |we currently support ${RUN_SPEC_TIMEOUT_ATTRS}
                                        |""".stripMargin.replaceAll("\n", ""))

        }
        return Some(DxTimeout(checkedParseIntField(subFields, "days"),
                              checkedParseIntField(subFields, "hours"),
                              checkedParseIntField(subFields, "minutes")))
    }

    private def parseRunSpec(jsv: JsValue) : Option[DxRunSpec] = {
        if (jsv == JsNull)
            return None
        val fields = jsv.asJsObject.fields
        for (k <- fields.keys) {
            if (!(RUN_SPEC_ATTRS contains k))
                throw new Exception(s"""|Unsupported runSpec attribute ${k},
                                        |we currently support ${RUN_SPEC_ATTRS}
                                        |""".stripMargin.replaceAll("\n", ""))

        }
        return Some(DxRunSpec(parseExecutionPolicy(checkedParseObjectField(fields, "executionPolicy")),
                              parseTimeoutPolicy(checkedParseObjectField(fields, "timeoutPolicy")),
                              parseAccess(checkedParseObjectField(fields, "access"))))
    }

    private def parseTaskDxAttrs(jsv: JsValue,
                                 verbose: Verbose) : Option[DxRunSpec] = {
        if (jsv == JsNull)
            return None
        val fields = jsv.asJsObject.fields
        for (k <- fields.keys) {
            if (!(TASK_DX_ATTRS contains k))
                throw new Exception(s"""|Unsupported runtime attribute ${k},
                                        |we currently support ${TASK_DX_ATTRS}
                                        |""".stripMargin.replaceAll("\n", ""))

        }
        return parseRunSpec(checkedParseObjectField(fields, "runSpec"))
    }


    def parse(jsv: JsValue,
              verbose: Verbose) : Extras = {
        val fields = jsv match {
            case JsObject(fields) => fields
            case _ => throw new Exception(s"malformed extras JSON ${jsv}")
        }

        // Guardrail, check the fields are actually supported
        for (k <- fields.keys) {
            if (!(EXTRA_ATTRS contains k))
                throw new Exception(s"""|Unsupported special option ${k},
                                        |we currently support ${EXTRA_ATTRS}
                                        |""".stripMargin.replaceAll("\n", ""))
        }
        Extras(parseRuntimeAttrs(
                   checkedParseObjectField(fields, "default_runtime_attributes"),
                   verbose),
               parseTaskDxAttrs(
                   checkedParseObjectField(fields, "default_task_dx_attributes"),
                   verbose))
    }
}
