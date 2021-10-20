package com.samihann.reducers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.util.StringTokenizer
import scala.collection.JavaConverters.*
import scala.util.matching.Regex

class JobTwoFinalReducer extends Reducer[IntWritable,Text,Text,IntWritable] {
  override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    val a = values.toString
    val newKey = new Text()
    newKey.set(a)
    context.write(newKey, key)

  }
}
