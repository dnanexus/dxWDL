package dxWDL.compiler

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
