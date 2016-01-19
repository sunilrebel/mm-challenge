package com.example

import com.twitter.scalding._
import App._

class App(args: Args) extends Job(args) {

  val eventInputPath = args(eventInputPathArg)
  val outputPath = args(outputArg)

  val eventSource = Csv(eventInputPath, fields = Event.schema).read
  eventSource
    .groupBy(Event.advertiser_id, Event.event_type) {
    group =>
      group.size
  }
    .project(Event.advertiser_id, Event.event_type, 'size)
    .write(Csv(outputPath))
}

object App {
  val eventInputPathArg = "eventInputPath"
  val outputArg = "output"
}
