package ours_LCPS.tool

import org.apache.log4j.{Level, Logger}

class log {
  def level(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
  }
}
