package com.example

import com.twitter.scalding._

class App(args: Args) extends Job(args) {

  val input = args("input")
}
