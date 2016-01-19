package com.example

import com.twitter.scalding.{Csv, JobTest}
import org.testng.annotations.Test
import App._

class AppTest {

  @Test(groups = Array("job"))
  def testApp() = {

    val eventsInput = List(
      ("1450501231", "325f8cc6-c4ae-42ad-895f-09fd231a8d79", "2", "7fe40811-7d3b-42b9-83ff-58eee06859d9", "purchase"),
      ("1450501449", "253ea62f-0fd2-4b54-bb9b-238df34ff2f5", "1", "6507f07f-be06-4355-9067-ab7dcba6a895", "click")
    )

    JobTest[App]
      .arg(eventInputPathArg, eventInputPathArg)
      .arg(outputArg, outputArg)
      .source(Csv(eventInputPathArg), eventsInput)
      .sink[(String)](Csv(outputArg)) {
      op =>
        val results = op.toList
        results.foreach(println)
        assert(results.size > 0)
    }
      .run
      .finish
  }
}
