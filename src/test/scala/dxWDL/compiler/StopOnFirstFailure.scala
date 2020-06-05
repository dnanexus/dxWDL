package dxWDL.compiler

import org.scalatest._
//import events.Event
//import events.TestSucceeded
import scala.collection.mutable.ListBuffer

trait StopOnFirstFailure extends SuiteMixin { this: Suite =>

  override def runTests(testName : Option[String], args: Args): Status = {

//    import args._

//    val stopRequested = stopper

    // If a testName is passed to run, just run that, else run the tests returned
    // by testNames.
    testName match {
      case Some(tn) => runTest(tn, args)
      case None =>
        val statusList = new ListBuffer[Status]()
        val tests = testNames.iterator
        var failed = false
        for (test <- tests) {
          if (failed == false) {
            val status = runTest(test, args)
            statusList += status
            failed = !status.succeeds()
          }
        }
        new CompositeStatus(Set.empty ++ statusList)
    }
  }
}
