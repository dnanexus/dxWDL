package dx.core.languages.wdl

import dx.api.InstanceTypeReq
import wdlTools.eval.WdlValues

object InstanceTypes {
  // Currently, we support only constants.
  def parse(dxInstanceType: Option[WdlValues.V],
            wdlMemoryMB: Option[WdlValues.V],
            wdlDiskGB: Option[WdlValues.V],
            wdlCpu: Option[WdlValues.V],
            wdlGpu: Option[WdlValues.V]): InstanceTypeReq = {
    // Shortcut the entire calculation, and provide the dx instance type directly
    dxInstanceType match {
      case None => None
      case Some(WdlValues.V_String(iType)) =>
        return InstanceTypeReq(Some(iType), None, None, None, None)
      case Some(x) =>
        throw new Exception(
            s"""|dxInstaceType has to evaluate to a
                |String type ${x}""".stripMargin
              .replaceAll("\n", " ")
        )
    }

    // Examples for memory specification: "4000 MB", "1 GB"
    val memoryMB: Option[Int] = wdlMemoryMB match {
      case None                          => None
      case Some(WdlValues.V_String(buf)) =>
        // extract number
        val numRex = """(\d+\.?\d*)""".r
        val numbers = numRex.findAllIn(buf).toList
        if (numbers.length != 1)
          throw new Exception(s"Can not parse memory specification ${buf}")
        val number: String = numbers.head
        val x: Double =
          try {
            number.toDouble
          } catch {
            case _: Throwable =>
              throw new Exception(s"Unrecognized number ${number}")
          }

        // extract memory units
        val memUnitRex = """([a-zA-Z]+)""".r
        val memUnits = memUnitRex.findAllIn(buf).toList
        if (memUnits.length > 1)
          throw new Exception(s"Can not parse memory specification ${buf}")
        val memBytes: Double =
          if (memUnits.isEmpty) {
            // specification is in bytes, convert to megabytes
            x.toInt
          } else {
            // Units were specified
            val memUnit: String = memUnits.head.toLowerCase
            val nBytes: Double = memUnit match {
              case "b"   => x
              case "kb"  => x * 1000d
              case "mb"  => x * 1000d * 1000d
              case "gb"  => x * 1000d * 1000d * 1000d
              case "tb"  => x * 1000d * 1000d * 1000d * 1000d
              case "kib" => x * 1024d
              case "mib" => x * 1024d * 1024d
              case "gib" => x * 1024d * 1024d * 1024d
              case "tib" => x * 1024d * 1024d * 1024d * 1024d
              case _     => throw new Exception(s"Unknown memory unit ${memUnit}")
            }
            nBytes.toDouble
          }
        val memMib: Double = memBytes / (1024 * 1024).toDouble
        Some(memMib.toInt)
      case Some(x) =>
        throw new Exception(s"Memory has to evaluate to a String type ${x}")
    }

    // Examples: "local-disk 1024 HDD"
    val diskGB: Option[Int] = wdlDiskGB match {
      case None => None
      case Some(WdlValues.V_String(buf)) =>
        val components = buf.split("\\s+")
        val ignoreWords = Set("local-disk", "hdd", "sdd", "ssd")
        val l = components.filter(x => !(ignoreWords contains x.toLowerCase))
        if (l.length != 1)
          throw new Exception(s"Can't parse disk space specification ${buf}")
        val i =
          try {
            l(0).toInt
          } catch {
            case _: Throwable =>
              throw new Exception(s"Parse error for diskSpace attribute ${buf}")
          }
        Some(i)
      case Some(x) =>
        throw new Exception(s"Disk space has to evaluate to a String type ${x}")
    }

    // Examples: "1", "12"
    val cpu: Option[Int] = wdlCpu match {
      case None => None
      case Some(WdlValues.V_String(buf)) =>
        val i: Int =
          try {
            buf.toInt
          } catch {
            case _: Throwable =>
              throw new Exception(s"Parse error for cpu specification ${buf}")
          }
        Some(i)
      case Some(WdlValues.V_Int(i))   => Some(i)
      case Some(WdlValues.V_Float(x)) => Some(x.toInt)
      case Some(x)                    => throw new Exception(s"Cpu has to evaluate to a numeric value ${x}")
    }

    val gpu: Option[Boolean] = wdlGpu match {
      case None                            => None
      case Some(WdlValues.V_Boolean(flag)) => Some(flag)
      case Some(x)                         => throw new Exception(s"Gpu has to be a boolean ${x}")
    }

    InstanceTypeReq(None, memoryMB, diskGB, cpu, gpu)
  }
}
