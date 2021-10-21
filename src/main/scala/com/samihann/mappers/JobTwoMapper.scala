package com.samihann.mappers

/*
Written by Samihan Nandedkar
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


/***
This is a Mapper for Job 2 to find the number of Error messages in a the time intervals and sort them in accending order.

Input: (Key: Object, Value: Text - Input File)
Output: (Key: Text - Message Type, Value: Int - Number of times this message type has pattern present in given time interval)


***/


class JobTwoMapper extends Mapper[Object, Text, Text, IntWritable] {

  // Import the configuration from configuration.conf
  val config: Config = ConfigFactory.load("configuration")
  // Declare Logger
  val log: Logger = LoggerFactory.getLogger(getClass)

  // Assign one and line as Hadoop datatypes
  val one = new IntWritable(1)
  val line = new Text()

  // Get the predifined Start and End time from configuration
  val timeInterval: Int = config.getInt("configuration.timeInterval")
  log.info("Time interval imported from Configuration")

  /***
   * Var is utilized here.
   * This is done as to calculate the multiple intervals of designated time duration.
   * The start time and end time dynamically changing as more lines are passed.
   * TO keep track of a single start and ent time for a particular interval var is used here.
   *
   */
  var st = new LocalTime()
  var et = new LocalTime(0,0,0,000)
  // Map Function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      log.info("Starting Map Function for Job 2")
      // Break the input value by lines.
      val itr = new StringTokenizer(value.toString,"\n")

      // Import the parameters from Configuration and convert it to regex through .r
      val logType = config.getString("configuration.messageType").r
      val pattern = config.getString("configuration.job2Pattern").r
      val time = config.getString("configuration.timeRegex").r
      log.info("Imported the parameters from Configuration and converted it to regex")

      // Iterated while new lines are present.
      while (itr.hasMoreTokens()) {
        val newLine = itr.nextToken() // Assign newLine the string for next log message
        log.info("Assigned newLine the string for next log message")
        // Find time stamp for the particualr log messages.
        val logTime = time.findFirstIn(newLine)
        // Find the messsge type for the particular log message
        val matchLog = logType.findFirstIn(newLine)
        // Find if the pattern is present in this particualr log message.
        val matchpresent = pattern.findFirstIn(newLine)
        log.info("Perform the checks to find the timestamp, messageType and pattern in the log message. ")

        // Check if the Time Stamp is present in the log message.
        logTime match {
          case Some(t) => {
            // Parse the time string
            val lt = LocalTime.parse(t)

            if (lt > et){
              st = lt
              et = st.plusMinutes(timeInterval)
            }
            // Check if the time lies in the given predefined time frame
            if((lt>=st) && (lt<=et)){
              matchLog match {
                case Some(s) => {
                  // Set line as the message Type
                  val x = et.toString()
                  val y = st.toString()
                  line.set(y+" TO "+x)
                  matchpresent match {
                    // If pattern is present for that message type
                    case Some(k) => {context.write(line,one)}
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
