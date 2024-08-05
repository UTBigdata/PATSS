package ours_LCPS.tool

import javassist.bytecode.analysis.Executor
import org.apache.hadoop.mapred.Master
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class sparkInitial {
  def start(partitionNum:String,appName:String,totalExeCores:String,master: String): SparkSession = {

    new log().level()

    val sparkConf = new SparkConf()
    sparkConf.
      set("spark.master",master).
      set("spark.cores.max",totalExeCores).
      set("spark.executor.cores","16").
      set("spark.executor.memory","30g").
      set("spark.driver.cores","16").
      set("spark.driver.memory","30g").
      set("spark.driver.maxResultSize", "30g").
      set("spark.default.parallelism", partitionNum).
      set("spark.shuffle.consolidateFiles", "true").
      set("spark.shuffle.memoryFraction", "0.8").
      set("spark.sql.shuffle.partitions", partitionNum).
      set("spark.executor.heartbeatInterval", "200000").
      set("spark.network.timeout", "300000")
    SparkSession.builder().config(sparkConf).appName(appName).getOrCreate()
  }
  def stop(sparkSession: SparkSession): Unit = {
    sparkSession.stop()
  }
}
