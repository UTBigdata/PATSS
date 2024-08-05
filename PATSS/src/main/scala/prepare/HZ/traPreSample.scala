package prepare.HZ

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.types.StringType
import ours_LCPS.tool.{log, sparkInitial}

import scala.collection.mutable.ArrayBuffer

object traPreSample {
  def main(args: Array[String]): Unit = {

    new log().level()

    val hdfs = args.head
    val master = args.last
    val spark = new sparkInitial().start("384","pre","192",master)

    import spark.implicits._

    val hz = spark.read.format("csv").option("header", value = true).
      load(s"hdfs://10.103.108.7:9000/HZ").// /HZ
      select("userID", "status","time", "lineID", "stationID", "payType").
      filter($"payType"=!=3).drop($"payType").na.drop().filter {x=>
        val con =  new ArrayBuffer[Boolean]()
        for(i <- x.toSeq.indices){
          con.append(x(i).asInstanceOf[String].nonEmpty)
        }
        con.reduce(_ && _)
      }.map{x=>
        val IO = x(1).asInstanceOf[String]
        val IO1 = if (IO.equals("1")) "I" else "O"
        val seq = x.toSeq
        val seq1 = seq.drop(2).+:(IO1).+:(seq.head).map(_.toString)
        (seq1.head,seq1(1),seq1(2),seq1(3),seq1(4))
      }.toDF("card","transactionType","transactionTime","line","station")
    /*+---------------------------------+---------------+-------------------+----+-------+
    |card                             |transactionType|transactionTime    |line|station|
    +---------------------------------+---------------+-------------------+----+-------+
    |C53addd517417f261b6f18ef5a4bfbd36|I              |2019-01-01 05:05:30|B   |14     |
    +---------------------------------+---------------+-------------------+----+-------+ */

    /*val hz0 = hz.select($"card", $"transactionType",$"transactionTime", $"line",
      concat_ws("<-",$"station",$"line").cast(StringType).as("station"))

    hz0.select($"line",$"station").distinct.show
    hz0.groupBy($"line").agg(collect_set($"station")).collect().foreach(println(_))*/

    val hz1 = hz.select($"card",
      concat_ws(".", $"transactionTime",$"station", $"line", $"transactionType").
        cast(StringType).as("point"))
    /*+---------------------------------+--------------------------+
    |card                             |point                     |
    +---------------------------------+--------------------------+
    |C53addd517417f261b6f18ef5a4bfbd36|2019-01-01 05:05:30.14.B.I|
    +---------------------------------+--------------------------*/

    val hz2: RDD[Row] = hz1.rdd.
      map { x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String]) }.
      reduceByKey(_ + "," + _).
      map { Tra => Row(Tra._1, Tra._2.split(",").sorted.mkString(",")) }.
      filter(t => t(1).asInstanceOf[String].split(",").length > 21 && t(1).asInstanceOf[String].split(",").length < 369)

    val hzLen = hz2.
      map { x => x(1).asInstanceOf[String].split(",").length }

    hzLen.count()
    hzLen.sum()/hzLen.count()

//    hz2.count() //613569
    val staTraOri1 = hz2.map(x => x(0) + "\t" + x(1))
    staTraOri1.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-100")
    val raw25 = staTraOri1.sample(withReplacement = false, 0.25).repartition(192)
    raw25.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-25")
    val raw50 = staTraOri1.sample(withReplacement = false, 0.5).repartition(192)
    raw50.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-50")
    val raw75 = staTraOri1.sample(withReplacement = false, 0.75).repartition(192)
    raw75.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-75")

    //mini sample
    val staTraOri0mini = hz1.rdd.
      map { x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String]) }.
      reduceByKey(_ + "," + _).
      map { Tra => Row(Tra._1, Tra._2.split(",").sorted.mkString(",")) }.
      filter(t => t(1).asInstanceOf[String].split(",").length > 21 && t(1).asInstanceOf[String].split(",").length < 23)
    val staTraOri1mini = staTraOri0mini.map(x => x(0) + "\t" + x(1))

    staTraOri1mini.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-mini-100")
    val raw25mini = staTraOri1mini.sample(withReplacement = false, 0.25).repartition(192)
    raw25mini.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-mini-25")
    val raw50mini = staTraOri1mini.sample(withReplacement = false, 0.5).repartition(192)
    raw50mini.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-mini-50")
    val raw75mini = staTraOri1mini.sample(withReplacement = false, 0.75).repartition(192)
    raw75mini.saveAsTextFile(s"$hdfs/ours/HZ/all/HZ-2019Jan-mini-75")

  }
}
