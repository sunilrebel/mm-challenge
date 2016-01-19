package com.example

object Event {

  def timestamp = "timestamp"
  def event_id = "event_id"
  def advertiser_id = "advertiser_id"
  def user_id = "user_id"
  def event_type = "event_type"

  val schema = List(
  timestamp,
  event_id,
  advertiser_id,
  user_id,
  event_type
  )
}
