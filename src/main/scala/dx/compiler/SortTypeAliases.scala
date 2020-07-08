package dx.compiler

import wdlTools.types.WdlTypes
import wdlTools.util.Logger

// sort the definitions by dependencies
case class SortTypeAliases(logger: Logger) {
  private val logger2: Logger = logger.withTraceIfContainsKey("SortTypeAliases")

  // figure out all the types that type [a] depends on. Ignore
  // standard types like Int, String, Array, etc.
  //
  private def dependencies(a: WdlTypes.T): Set[String] = {
    a match {
      // Base case: primitive types.
      case WdlTypes.T_Boolean | WdlTypes.T_Int | WdlTypes.T_Float | WdlTypes.T_String |
          WdlTypes.T_File | WdlTypes.T_Directory =>
        Set.empty

      // compound types
      case WdlTypes.T_Array(memberType, _) =>
        dependencies(memberType)
      case WdlTypes.T_Map(keyType, valueType) =>
        dependencies(keyType) ++ dependencies(valueType)
      case WdlTypes.T_Optional(memberType) =>
        dependencies(memberType)
      case WdlTypes.T_Pair(lType, rType) =>
        dependencies(lType) ++ dependencies(rType)

      // structs
      case WdlTypes.T_Struct(structName, typeMap) =>
        typeMap.foldLeft(Set(structName)) {
          case (accu, (_, t)) =>
            accu ++ dependencies(t)
        }

      // catch-all for other types not currently supported
      case other =>
        throw new Exception(s"Unsupported WDL type ${other}")
    }
  }

  private def dependenciesOuter(a: WdlTypes.T): Set[String] = {
    val d = dependencies(a)
    a match {
      case WdlTypes.T_Struct(name, _) => d - name
      case _                          => d
    }
  }

  private def next(remaining: Vector[WdlTypes.T],
                   alreadySorted: Vector[WdlTypes.T]): (Vector[WdlTypes.T], Vector[WdlTypes.T]) = {
    val alreadySortedNames: Set[String] = alreadySorted.collect {
      case WdlTypes.T_Struct(name, _) => name
    }.toSet

    val (zeroDeps, top) = remaining.partition {
      case a: WdlTypes.T_Struct =>
        val deps = dependenciesOuter(a)
        logger2.trace(s"dependencies(${a}) === ${deps}")
        deps.subsetOf(alreadySortedNames)
      case _ => true
    }
    assert(zeroDeps.nonEmpty)
    (zeroDeps, top)
  }

  private def getNames(ta: Vector[WdlTypes.T]): Vector[String] = {
    ta.flatMap {
      case WdlTypes.T_Struct(name, _) => Some(name)
      case _                          => None
    }
  }

  private def apply2(typeAliases: Vector[WdlTypes.T]): Vector[WdlTypes.T] = {
    val N = typeAliases.size

    var accu = Vector.empty[WdlTypes.T]
    var crnt = typeAliases

    while (crnt.nonEmpty) {
      logger2.trace(s"accu=${getNames(accu)}")
      logger2.trace(s"crnt=${getNames(crnt)}")
      val (zeroDeps, top) = next(crnt, accu)
      accu = accu ++ zeroDeps
      crnt = top
    }
    assert(accu.length == N)
    accu
  }

  def apply(typeAliases: Vector[(String, WdlTypes.T)]): Vector[(String, WdlTypes.T)] = {
    val justTypes = typeAliases.map { case (_, x) => x }
    val sortedTypes = apply2(justTypes)

    sortedTypes.map {
      case a @ WdlTypes.T_Struct(name, _) => (name, a)
      case other                          => throw new Exception(s"Unexpected type ${other}")
    }
  }
}
