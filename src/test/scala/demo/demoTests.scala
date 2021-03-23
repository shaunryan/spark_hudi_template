package demo

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class DemoTests extends AnyFunSuite with BeforeAndAfter {
  
  var stub: Boolean = true

  before {
    stub = true
  }

  test("new true") {
    assert(stub)
  }


  // mark that you want a test here in the future
//   test ("test pizza pricing") (pending)

}