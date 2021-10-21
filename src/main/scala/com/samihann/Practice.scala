package com.samihann

import java.util.StringTokenizer
import scala.util.matching.Regex
import org.apache.hadoop.io.{IntWritable, Text}
import org.joda.time.LocalTime
import com.typesafe.config.*
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.joda.time.LocalTime
import com.github.nscala_time.time.Imports.*

import java.util.StringTokenizer
import com.typesafe.config.*
import org.apache.hadoop.fs.Path

import java.lang.Iterable
import scala.collection.JavaConverters._

object Practice{


  val t = "20:04:33.070"
  val lt = LocalTime.parse(t)
  var st = new LocalTime()
  var et = new LocalTime(0,0,0,000)
  if (lt > et){
    st = lt
    et = st.plusMinutes(5)
  }
  print(lt,st,et)
  val t1 = "20:04:37.119"
  val lt1 = LocalTime.parse(t1)
  if (lt1 > et){
    print("here again")
    st = lt1
    et = st.plusMinutes(5)
  }
  print(lt1,st,et)



}