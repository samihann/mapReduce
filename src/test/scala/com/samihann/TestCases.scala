package com.samihann

import com.samihann.Utility.TimeParserUtility

import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.joda.time.LocalTime
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters.*


class TestCases extends AnyFunSuite {

  val message1 = "19:54:52.408 [scala-execution-context-global-13] INFO  HelperUtils.Parameters$ - -Efd6A9F:{M*[Vwh(Ys+rHuc\".t*12\",[.;[j?2o2.6y4Wkr^2x\\jfAH"
  val message2 = "19:54:52.517 [scala-execution-context-global-13] DEBUG HelperUtils.Parameters$ - 8:Wl}c0L\\>EtwOu^S}0\"kS\"_?bX#"


  test("Test to check if the Logger is getting congifured.") {
    val log = LoggerFactory.getLogger(getClass)
    assert(log.isInstanceOf[Logger])
  }

  test ("Test to check if configuration file is present"){
    val config: Config = ConfigFactory.load("configuration")
    assert(!config.isEmpty)
  }

  test("Test to check if datatypes are getting converted succesfully from Hadoop  to Scala"){
  val test = new Text()
  val textString = test.toString
  val test1 = new IntWritable()
  val textInt = test1.get()
  assert(textString.isInstanceOf[String])
  assert(textInt.isInstanceOf[Int])
  }

  test("Test to check TimeParserUtility file created return Locatime value"){
    val tmutility = new TimeParserUtility("19:54:52.408")
    val lt = tmutility.parse
    assert(lt.isInstanceOf[LocalTime])
  }

  test("Test to check if pattern is being recognised in the log message if present"){
    val pattern = "Efd".r
    val matchCheck = pattern.findFirstIn(message1)
    matchCheck match {
      case Some(s) => assert(!s.isEmpty)
      case None =>
    }
  }

  test("Test to check TimeStamp is being recognised in the log message"){
    val time = """([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3})""".r
    val matchCheck = time.findFirstIn(message1)
    matchCheck match {
      case Some(s) => {
        val timeStamp = LocalTime.parse(s)
        assert(timeStamp.isInstanceOf[LocalTime])
      }
      case None =>
    }
  }

  test("Test to check if log message type is being recognised in the leg message type"){
    val mtype = "ERROR|INFO|DEBUG|WARN".r
    val matchCheck = mtype.findFirstIn(message1)
    matchCheck match {
      case Some(s) => {
        assert(!s.isEmpty)
      }
    }

  }


}