package dx

// Exception used for AppInternError
class AppInternalException(message: String) extends RuntimeException(message)

// Exception used for AppError
class AppException(message: String) extends RuntimeException(message)

class PermissionDeniedException(message: String) extends Exception(message) {
  def this(message: String, cause: Throwable) = {
    this(message)
    initCause(cause)
  }
}

class InvalidInputException(s: String) extends Exception(s)

class IllegalArgumentException(s: String) extends Exception(s)
