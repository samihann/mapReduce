package com.samihann.mappers

import com.github.nscala_time.time.Imports.*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.joda.time.LocalTime
import org.slf4j.{Logger, LoggerFactory}

import java.lang.Iterable
import java.util.StringTokenizer
import scala.util.matching.Regex


/***
Mapper for Job 2 to find the arrange the time intervals in decending order.
Input: (Key: Object, Value: Text - Input File)
Output: (Key: Text - Message Type, Value: Int - Number of times this message type has pattern present in given time interval)
***/


class JobTwoFinalMapper extends Mapper[Text, IntWritable, IntWritable, Text] {

  // Import the configuration from configuration.conf
  val config: Config = ConfigFactory.load("configuration")
  // Declare Logger
  val log: Logger = LoggerFactory.getLogger(getClass)


    override def map(key: Text, value: IntWritable, context: Mapper[Text, IntWritable, IntWritable, Text]#Context): Unit = {
      log.info("Starting Map Function")
//      val itr = new StringTokenizer(value.toString,"\n")
//      while (itr.hasMoreTokens()) {
//        val newLine = itr.nextToken()
//        val array = newLine.toString
//        val newValue = new Text()
//        newValue.set(array)
//        context.write(new IntWritable(1),newValue)
//      }

      context.write(value,key)


    }

}
