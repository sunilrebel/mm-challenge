package com.example

import com.twitter.scalding.{Csv, JobTest}
import org.testng.annotations.Test

class AppTest {

  @Test(groups = Array("job"))
  def testApp() = {

    val inputFields = List(("abc", "hello"))

    JobTest[App]
      .arg("input", "input")
      .source(Csv("input"), inputFields)
      .sink[(String)](Csv("output")) {
      op =>
        val results = op.toList
        assert(results.size > 0)
    }
      .run
      .finish
  }
}
