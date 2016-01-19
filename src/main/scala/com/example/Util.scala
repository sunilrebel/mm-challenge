package com.example

object Util {

  def getFile(filename: String) = {
    this.getClass.getResource(filename).getPath
  }
}
