package com.samihann.Utility

import org.joda.time.LocalTime
import com.github.nscala_time.time.Imports.*
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

class TimeParserUtility (var time: String) {

  val lt = time
  def parse: LocalTime ={
    val time = LocalTime.parse(lt)
    return time
  }

}
