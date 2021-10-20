package com.samihann.mappers

/*
Created by Samihan Nandedkar
CS441 | Fall 2021
*/
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


/**
Mapper for Job 3 to find the total number of times regex pattern is repeated across the log files grouping by message type.

Input: (Key: Object, Value: Text - Input File)
Output: (Key: Text - Message Type, Value: Int - Number of times this message type has pattern present)

**/


class JobThreeMapper extends Mapper[Object, Text, Text, IntWritable] {

  // Import the configuration from configuration.conf
  val config: Config = ConfigFactory.load("configuration")
  // Declare Logger
  val log: Logger = LoggerFactory.getLogger(getClass)

  // Assign one and line as Hadoop datatypes
  val one = new IntWritable(1)
  val line = new Text()


  // Map Function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      log.info("Starting Map Function For JOB 3")
      // Break the input value by lines.
      val itr = new StringTokenizer(value.toString,"\n")

      // Import the parameters from Configuration and convert it to regex through .r
      val logType = config.getString("configuration.messageType").r
      val pattern = config.getString("configuration.pattern").r
      val time = config.getString("configuration.timeRegex").r
      log.info("Imported the parameters from Configuration and converted it to regex")

      // Iterated while new lines are present.
      while (itr.hasMoreTokens()) {
        val newLine = itr.nextToken() // Assign newLine the string for next log message
        log.info("Assigned the newLine the string for next log message")
        // Find the message type for the particular log message
        val matchLog = logType.findFirstIn(newLine)
        // Find if the pattern is present in this particualr log message.
        val matchpresent = pattern.findFirstIn(newLine)
        log.info("Perform the checks to find the timestamp, messageType and pattern in the log message. ")
        matchLog match {
          case Some(s) => {
            // Set line as the message Type
            line.set(s)
            matchpresent match {
              // If pattern is present for that message type
              case Some(k) => {
                context.write(line, one)
              }
              case None => {
                log.info("No match in this line")
              }
            }
          }
          case None => {
            log.info("No message type")
          }
        }
      }
    }

}
