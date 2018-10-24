package dxWDL.compiler

import dxWDL.util.{Utils, Verbose}
import wom.types._

// The WDL source variable with a fully qualified name. It can include for example,
// "add.result", "mul.result".
case class DVar(fullyQualifiedName: String,
                womType: WomType) {
    def unqualifiedName : String = {
        val index = fullyQualifiedName.lastIndexOf('.')
        if (index == -1)
            fullyQualifiedName
        else
            fullyQualifiedName.substring(index + 1)
    }
}

case class NameBox(verbose: Verbose) {
    // never allow the reserved words
    private var allExecNames:Set[String] = Utils.RESERVED_WORDS

    def chooseUniqueName(baseName: String) : String = {
        if (!(allExecNames contains baseName)) {
            allExecNames += baseName
            return baseName
        }

        // Add [1,2,3 ...] to the original name, until finding
        // an unused variable name
        for (i <- 1 to Utils.DECOMPOSE_MAX_NUM_RENAME_TRIES) {
            val tentative = s"${baseName}${i}"
            if (!(allExecNames contains tentative)) {
                allExecNames += tentative
                return tentative
            }
        }
        throw new Exception(s"Could not find a unique name for applet ${baseName}")
    }

    def add(name:String) : Unit = {
        allExecNames += name
    }
}
