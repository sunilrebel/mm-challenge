package com.example

import com.twitter.scalding._
import App._

class App(args: Args) extends Job(args) {

  val eventInputPath = args(eventInputPathArg)
  val outputEventPath = args(outputEventArg)
  val outputUserPath = args(outputUserArg)

  val eventSource = Csv(eventInputPath, fields = Event.schema).read

  eventSource
    .groupBy(Event.advertiser_id, Event.event_type) {
    group =>
      group.size
  }
    .project(Event.advertiser_id, Event.event_type, 'size)
    .write(Csv(outputEventPath))

  eventSource
    .groupBy(Event.advertiser_id, Event.event_type) {
    group =>
      group.size
  }
    .project(Event.advertiser_id, Event.event_type, 'size)
    .write(Csv(outputUserPath))
}

object App {
  val eventInputPathArg = "eventInputPath"
  val outputEventArg = "outputEventPath"
  val outputUserArg = "outputUserArg"
}
