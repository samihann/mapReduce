package com.samihann.reducers

/*
Create by Samihan Nandedkar
CS441 | Fall 2021
*/

import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex
import scala.collection.JavaConverters.*

class JobOneReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  // Logger
  val log: Logger = LoggerFactory.getLogger(getClass)

  // main Reduce Function
  override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    log.info("Starting the Reducer for Job 1")
    /***
     * values is iterable structure containing character sizes
     * for all the messages for each type.
     *
     * We are iterating through the list with foldLeft and adding them with each other
     * using (_ + _.get)
     *
     * .get in above given expression is given to convert IntWritable to Int.
     *
     *
     */
    val sum = values.asScala.foldLeft(0)(_ + _.get)

    // Write the values in Context.
    context.write(key, new IntWritable(sum))
  }
}
