package dxWDL.compiler

import wom.types._
import dxWDL.base.{Utils, Verbose}

// sort the definitions by dependencies
case class SortTypeAliases(verbose: Verbose) {
    private val verbose2:Boolean = verbose.containsKey("SortTypeAliases")

    // figure out all the types that type [a] depends on. Ignore
    // standard types like Int, String, Array, etc.
    //
    private def dependencies(a : WomType) : Set[String] = {
        a match {
            // Base case: primitive types.
            case WomNothingType
                   | WomBooleanType
                   | WomIntegerType
                   | WomLongType
                   | WomFloatType
                   | WomStringType
                   | WomSingleFileType => Set.empty

            // compound types
            case WomMaybeEmptyArrayType(memberType) =>
                dependencies(memberType)
            case WomMapType(keyType, valueType) =>
                dependencies(keyType) ++ dependencies(valueType)
            case WomNonEmptyArrayType(memberType) =>
                dependencies(memberType)
            case WomOptionalType(memberType) =>
                dependencies(memberType)
            case WomPairType(lType, rType) =>
                dependencies(lType) ++ dependencies(rType)

            // structs
            case WomCompositeType(typeMap, Some(structName)) =>
                typeMap.foldLeft(Set(structName)) {
                    case (accu, (_, t)) =>
                        accu ++ dependencies(t)
                }

            // catch-all for other types not currently supported
            case other =>
                throw new Exception(s"Unsupported WOM type ${other}, ${other.stableName}")
        }
    }

    private def dependenciesOuter(a : WomType) : Set[String] = {
        val d = dependencies(a)
        a match {
            case WomCompositeType(_, Some(name)) => d - name
            case _ => d
        }
    }

    private def next(remaining: Vector[WomType],
                     alreadySorted: Vector[WomType]) : (Vector[WomType], Vector[WomType]) = {
        val alreadySortedNames : Set[String] = alreadySorted.collect{
            case WomCompositeType(_, Some(name)) => name
        }.toSet

        val (zeroDeps, top) = remaining.partition {
            case a : WomCompositeType =>
                val deps = dependenciesOuter(a)
                Utils.trace(verbose2, s"dependencies(${a}) === ${deps}")
                deps.subsetOf(alreadySortedNames)
            case _ => true
        }
        assert(!zeroDeps.isEmpty)
        (zeroDeps.toVector, top.toVector)
    }

    private def getNames(ta : Vector[WomType]) : Vector[String] = {
        ta.map{
            case WomCompositeType(_, Some(name)) => name
        }.toVector
    }

    private def apply2(typeAliases: Vector[WomType]) : Vector[WomType] = {
        val N = typeAliases.size

        var accu = Vector.empty[WomType]
        var crnt = typeAliases

        while (!crnt.isEmpty) {
            Utils.trace(verbose2, s"accu=${getNames(accu)}")
            Utils.trace(verbose2, s"crnt=${getNames(crnt)}")
            val (zeroDeps, top) = next(crnt, accu)
            accu = accu ++ zeroDeps
            crnt = top
        }
        assert(accu.length == N)
        accu
    }


    def apply(typeAliases: Vector[(String, WomType)]) : Vector[(String, WomType)] = {
        val justTypes = typeAliases.map{ case (_,x) => x }
        val sortedTypes = apply2(justTypes)

        sortedTypes.map{
            case a@WomCompositeType(_, Some(name)) =>
                (name, a)
            case _ => throw new Exception("sanity")
        }.toVector
    }
}
