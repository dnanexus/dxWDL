package dxWDL

import java.io.PrintStream
import org.scalatest.{FlatSpec, Matchers}

/**Writes to nowhere*/
class NullOutputStream extends java.io.OutputStream {
    override def write(b: Int) : Unit = {
    }
}

class DxFunctionsTest extends FlatSpec with Matchers {

    it should "implement glob correctly" in {
        DxFunctions.setErrStream(new PrintStream(new NullOutputStream()))
        //DxFunctions.setErrStream(null)

        DxFunctions.globHelper("*.txt") should equal(Seq.empty[String])
    }
}
