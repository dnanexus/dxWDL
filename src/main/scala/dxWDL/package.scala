package dxWDL

// Exception used for AppInternError
class AppInternalException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

// Exception used for AppError
class AppException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

class UnboundVariableException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(varName: String) = this(new RuntimeException(s"Variable ${varName} is unbound"))
}

// Mode of file data transfer
//   Data: download of upload the entire file
//   Remote: leave the file on the platform
//   Stream: stream download/upload the file
object IOMode extends Enumeration {
    val Data, Remote, Stream = Value
}

object IORef extends Enumeration {
    val Input, Output = Value
}

// The direction of IO:
//   Download: downloading files
//   Upload:  uploading files
//   Zero:    no upload or download should be attempted
object IODirection extends Enumeration {
    val Download, Upload, Zero = Value
}

object CompilerFlag extends Enumeration {
    val Default, IR = Value
}

// Request for an instance type
case class InstanceTypeReq(dxInstanceType: Option[String],
                           memoryMB: Option[Int],
                           diskGB: Option[Int],
                           cpu: Option[Int])


// Encapsulation of verbosity flags.
//  on --       is the overall setting true/false
//  keywords -- specific words to trace
//  quiet:      if true, do not print warnings and informational messages
case class Verbose(on: Boolean,
                   quiet: Boolean,
                   keywords: Set[String])


// Packing of all compiler flags in an easy to digest
// format
case class CompilerOptions(archive: Boolean,
                           compileMode: CompilerFlag.Value,
                           defaults: Option[java.nio.file.Path],
                           force: Boolean,
                           locked: Boolean,
                           inputs: List[java.nio.file.Path],
                           reorg: Boolean,
                           verbose: Verbose)
