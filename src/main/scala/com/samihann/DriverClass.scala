package com.samihann

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import com.samihann.mappers.{JobFourMapper, JobOneMapper, JobThreeMapper, JobTwoFinalMapper, JobTwoMapper}
import com.samihann.reducers.{JobFourReducer, JobOneReducer, JobThreeReducer, JobTwoFinalReducer, JobTwoReducer}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl

object DriverClass {
  def main(args: Array[String]): Unit = {
    // Create Logger
    val log: Logger = LoggerFactory.getLogger(getClass)



    log.info("********** Job 1 to be executed *************")
    // Job 1: Perform Map Reduce on the input
    // Instance of Hadoop Configuration
    val JobOneConf = new Configuration
    // To generate the output of Map Reduce as Scala.
    JobOneConf.set("mapred.textoutputformat.separatorText", ",")
    val job1 = Job.getInstance(JobOneConf,"Pattern Search in a time frame")
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[JobOneMapper])
    job1.setCombinerClass(classOf[JobOneReducer])
    job1.setReducerClass(classOf[JobOneReducer])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputKeyClass(classOf[Text]);
    job1.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1)+"/job1"))
    log.info("********** EXECUTION OF JOB 1 COMPLETED *************")


    log.info("********** Job 2 to be executed  *************")
    // Job 2: Perform Map Reduce on the input
    // Instance of Hadoop Configuration
    val JobTwoConf = new Configuration
    // To generate the output of Map Reduce as Scala.
    JobTwoConf.set("mapred.textoutputformat.separatorText", ",")
    val job2 = Job.getInstance(JobTwoConf,"Pattern Search across the file")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[JobTwoMapper])
    job2.setCombinerClass(classOf[JobTwoReducer])
    job2.setReducerClass(classOf[JobTwoReducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text]);
    job2.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job2, new Path(args(0)))
    FileOutputFormat.setOutputPath(job2,  new Path(args(1)+"/job2"))
    val jobTwoControl = new ControlledJob(JobTwoConf)


//    log.info("********** STARTING EXECUTION OF JOB 2 Final  *************")
//    // Job 2: Perform Map Reduce on the input
//    // Instance of Hadoop Configuration
//    val JobTwoFConf = new Configuration
//    // To generate the output of Map Reduce as Scala.
//    JobTwoFConf.set("mapred.textoutputformat.separatorText", ",")
//    val job2f = Job.getInstance(JobTwoFConf,"Pattern Search across the file")
//    job2f.setJarByClass(this.getClass)
//    job2f.setMapperClass(classOf[JobTwoFinalMapper])
//    job2f.setCombinerClass(classOf[JobTwoFinalReducer])
//    job2f.setReducerClass(classOf[JobTwoFinalReducer])
//    job2f.setInputFormatClass(classOf[KeyValueTextInputFormat]);
//    job2f.setOutputKeyClass(classOf[Text])
//    job2f.setOutputKeyClass(classOf[Text]);
//    job2f.setOutputValueClass(classOf[IntWritable]);
//    FileInputFormat.addInputPath(job2f,  new Path(args(1)+"/job2/intermediate"))
//    FileOutputFormat.setOutputPath(job2f,  new Path(args(1)+"/job2/final"))
//    val jobTwoFControl = new ControlledJob(JobTwoFConf)
//    log.info("********** EXECUTION OF JOB 2 Final COMPLETED *************")
//
//    jobTwoFControl.addDependingJob(jobTwoControl)
//    val jcontrol: JobControl = new JobControl("Name")
//    jcontrol.addJob(jobTwoControl)
//    jcontrol.addJob(jobTwoFControl)
//
//    val runJControl: Thread = new Thread(jcontrol);
//    runJControl.start();


    log.info("********** JOB 3 to be executed *************")
    // Job 3: Perform Map Reduce on the input
    // Instance of Hadoop Configuration
    val JobThreeConf = new Configuration
    // To generate the output of Map Reduce as Scala.
    JobThreeConf.set("mapred.textoutputformat.separatorText", ",")
    val job3 = Job.getInstance(JobThreeConf,"Pattern Search across the file")
    job3.setJarByClass(this.getClass)
    job3.setMapperClass(classOf[JobThreeMapper])
    job3.setCombinerClass(classOf[JobThreeReducer])
    job3.setReducerClass(classOf[JobThreeReducer])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputKeyClass(classOf[Text]);
    job3.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job3, new Path(args(0)))
    FileOutputFormat.setOutputPath(job3, new Path(args(1)+"/job3"))



    log.info("********** JOB 4 to be executed *************")
    // Job 3: Perform Map Reduce on the input
    // Instance of Hadoop Configuration
    val JobFourConf = new Configuration
    // To generate the output of Map Reduce as Scala.
    JobFourConf.set("mapred.textoutputformat.separatorText", ",")
    val job4 = Job.getInstance(JobFourConf,"Pattern Search across the file")
    job4.setJarByClass(this.getClass)
    job4.setMapperClass(classOf[JobFourMapper])
    job4.setCombinerClass(classOf[JobFourReducer])
    job4.setReducerClass(classOf[JobFourReducer])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputKeyClass(classOf[Text]);
    job4.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job4, new Path(args(0)))
    FileOutputFormat.setOutputPath(job4, new Path(args(1)+"/job4"))


    System.exit(if((job2.waitForCompletion(true))&&(job1.waitForCompletion(true)) && (job4.waitForCompletion(true)) && (job3.waitForCompletion(true)) )  0 else 1)
  }

}

// (job3.waitForCompletion(true)) && (job1.waitForCompletion(true)) &&  && (job4.waitForCompletion(true))