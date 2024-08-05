package prepare.SZT

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import ours_LCPS.tool.{log, sparkInitial}

import java.text.SimpleDateFormat
import scala.language.postfixOps

object traPreSample {
  case class subWayICA(card: String, transactionType: String, transactionTime: String, station: String, line: String)

  def main(args: Array[String]): Unit = {

    new log().level()

    val hdfs = args.head //"hdfs://10.29.74.178:9000"
    val master = args.last //"spark://10.29.74.178:7077"
    new log().level()

    val spark = new sparkInitial().start("384", "traPre", "192", master)

    import spark.implicits._

    val szt = spark.read.format("csv").option("header", value = true).
      load("hdfs://10.29.74.178:9000/subway-data/SZT_200/szt/201811/szt_201811*").
      select("卡号", "交易类型", "交易日期时间", "线路站点", "公司名称").
      toDF("card", "transactionType", "transactionTime", "station", "line").na.drop().as[subWayICA]

    val sztPolice0 = szt.filter { ic =>
      (ic.transactionType.equals("地铁入站") || ic.transactionType.equals("地铁出站")) &&
        ic.card.nonEmpty &&
        ic.transactionTime.nonEmpty &&
        ic.station.nonEmpty &&
        ic.line.nonEmpty
    }
    /*
    +---------+---------------+---------------+-------+----------+
    |card     |transactionType|transactionTime|station|line      |
    +---------+---------------+---------------+-------+----------+
    |668379435|地铁入站       |20181031224559 |大剧院 |地铁二号线|
    +---------+---------------+---------------+-------+----------+
     */

    val szt1 = sztPolice0.map { ic =>

      val time1 = new SimpleDateFormat("yyyyMMddHHmmss")
      val time3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timef2 = time3.format(time1.parse(ic.transactionTime).getTime)
      val timeWrapped = timef2 //不做时间规整！！！！！！！！
      //      val timeWrapped = sztPoliceTime(ic.transactionTime)._1   //10min的时间规整

      var station = ic.station
      if (!station.endsWith("站")) station = station + "站"
      if (station.equals("马鞍山站")) station = "马安山站"
      else if (station.equals("深圳大学站")) station = "深大站"
      else if (station.equals("?I岭站")) station = "孖岭站"
      else station = station

      var inOut = ic.transactionType
      if (inOut.equals("地铁入站")) inOut = "I"
      else inOut = "O"

      (ic.card, inOut, timeWrapped, station, ic.line.drop(2))
    }.toDF("card", "transactionType", "transactionTime", "station", "line").as[subWayICA]
    /*
    +---------+---------------+-----------------+--------+------+
    |     card|transactionType|  transactionTime| station|  line|
    +---------+---------------+-----------------+--------+------+
    |668379435|              I|2018-10-31 22:45:59|大剧院站|二号线|
    +---------+---------------+-----------------+--------+------+
    */

    val staOri = szt1.select($"card",
      concat_ws(".", $"transactionTime", $"station", $"line", $"transactionType").
        cast(StringType).as("point"))
    /*
    +---------+-----------------------------------+
    |card     |point                              |
    +---------+-----------------------------------+
    |668379435|2018-10-31 22:45:59.大剧院站.二号线.I|
    +---------+-----------------------------------+
    */

    val staTraOri0: RDD[Row] = staOri.rdd.
      map { x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String]) }.
      reduceByKey(_ + "," + _).
      map { Tra => Row(Tra._1, Tra._2.split(",").sorted.mkString(",")) }.
      filter(t => t(1).asInstanceOf[String].split(",").length > 21 && t(1).asInstanceOf[String].split(",").length < 369)


    val szLen = staTraOri0.
      map { x => x(1).asInstanceOf[String].split(",").length }

    szLen.count()
    szLen.sum() / szLen.count()


    val staTraOri1 = staTraOri0.map(x=>x(0)+"\t"+x(1))

    staTraOri1.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-100")
    val raw25 = staTraOri1.sample(withReplacement = false, 0.25).repartition(192)
    raw25.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-25")
    val raw50 = staTraOri1.sample(withReplacement = false, 0.5).repartition(192)
    raw50.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-50")
    val raw75 = staTraOri1.sample(withReplacement = false, 0.75).repartition(192)
    raw75.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-75")

    //mini sample
    val staTraOri0mini = staOri.rdd.
      map { x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String]) }.
      reduceByKey(_ + "," + _).
      map { Tra => Row(Tra._1, Tra._2.split(",").sorted.mkString(",")) }.
      filter(t => t(1).asInstanceOf[String].split(",").length > 21 && t(1).asInstanceOf[String].split(",").length < 23)
    val staTraOri1mini = staTraOri0mini.map(x => x(0) + "\t" + x(1))

    staTraOri1mini.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-mini-100")
    val raw25mini = staTraOri1mini.sample(withReplacement = false, 0.25).repartition(192)
    raw25mini.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-mini-25")
    val raw50mini = staTraOri1mini.sample(withReplacement = false, 0.5).repartition(192)
    raw50mini.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-mini-50")
    val raw75mini = staTraOri1mini.sample(withReplacement = false, 0.75).repartition(192)
    raw75mini.saveAsTextFile(s"$hdfs/ours/SZT/all/SZT-2018Nov-mini-75")

  }
}
