package com.samihann.mappers

/*
Written by Samihan Nandedkar
CS441 | Fall 2021
*/

import java.lang.Iterable
import java.util.StringTokenizer

import org.joda.time.LocalTime
import com.github.nscala_time.time.Imports.*
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.util.matching.Regex


/***
This is Mapper for Job 1 to find the number of times regex pattern is repeated thorugh the given time frame.

Input: (Key: Object, Value: Text - Input File)
Output: (Key: Text - Message Type, Value: Int - Number of times this message type has pattern present in given time interval)


***/


class JobOneMapper extends Mapper[Object, Text, Text, IntWritable] {

  // Import the configuration from configuration.conf
  val config: Config = ConfigFactory.load("configuration")
  // Declare Logger
  val log: Logger = LoggerFactory.getLogger(getClass)

  // Assign one and line as Hadoop datatypes
  val one = new IntWritable(1)
  val line = new Text()

  // Get the predifined Start and End time from configuration
  val startTime: String = config.getString("configuration.startTime")
  val endTime: String = config.getString("configuration.endTime")
  val st =  LocalTime.parse(startTime)
  val et = LocalTime.parse(endTime)
  log.info("Start and End time imported from Configuration")

  // Map Function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      log.info("Starting Map Function for JOB 1")
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
        log.info("Assigned newLine the string for next log message")
        // Find time stamp for the particualr log messages.
        val logTime = time.findFirstIn(newLine)
        // Find the message type for the particular log message
        val matchLog = logType.findFirstIn(newLine)
        // Find if the pattern is present in this particualr log message.
        val matchpresent = pattern.findFirstIn(newLine)
        log.info("Perform the checks to find the timestamp, messageType and pattern in the log message. ")

        // Check if the Time Stamp is present in the log message.
        logTime match {
          case Some(t) => {
            // Parse the time string
            val lt = LocalTime.parse(t)
            // Check if the time lies in the given predefined time frame
            if((lt>=st) && (lt<=et)){
              matchLog match {
                case Some(s) => {
                  // Set line as the message Type
                  line.set(s)
                  matchpresent match {
                    // If pattern is present for that message type
                    case Some(k) => {context.write(line,one)}
                    // None will be returned if no match is present
                    case None => {log.info("No match in this line")}
                  }
                }
                case None => {log.info("No message type")}
              }
            }
          }
          case None => {
            log.info("Does not lie in the time interval")
          }
        }
      }
    }

}
