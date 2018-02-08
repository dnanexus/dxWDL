package dxWDL

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
