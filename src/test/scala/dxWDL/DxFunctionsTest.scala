package dxWDL

import java.io.PrintStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

/**Writes to nowhere*/
class NullOutputStream extends java.io.OutputStream {
    override def write(b: Int) : Unit = {
    }
}

class DxFunctionsTest extends FlatSpec with BeforeAndAfterEach {

    it should "implement glob correctly" in {
        DxFunctions.setErrStream(new PrintStream(new NullOutputStream()))
        //DxFunctions.setErrStream(null)

        DxFunctions.glob("/tmp/K", "*.txt")
        DxFunctions.glob("/tmp", "*.txt")
    }
}
