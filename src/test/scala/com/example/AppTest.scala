package com.example

import com.twitter.scalding.{TextLine, Csv, JobTest}
import org.testng.annotations.Test
import App._

class AppTest {

  @Test(groups = Array("job"))
  def testApp() = {

    val eventsInput = getEventData("/Users/tkmabvj/sunil/projects/mm-challenge/src/test/resources/events.csv")
    val impressionInput = getImpressionData("/Users/tkmabvj/sunil/projects/mm-challenge/src/test/resources/impressions.csv")

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

  def getEventData(filepath: String) = {
    scala.io.Source.fromFile(filepath).getLines().filter(!_.isEmpty).map {
      line =>
        val parts = line.split(",")
        (parts(0), parts(1), parts(2), parts(3), parts(4))
    }.toList
  }

  def getImpressionData(filepath: String) = {
    scala.io.Source.fromFile(filepath).getLines().filter(!_.isEmpty).map {
      line =>
        val parts = line.split(",")
        (parts(0), parts(1), parts(2), parts(3))
    }.toList
  }
}
