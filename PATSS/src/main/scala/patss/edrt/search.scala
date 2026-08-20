package patss.edrt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import opt.{candiSig, measure, refineCon, comTraFilter}
import tool.{Timer, attrGet, localPar, sparkInitial, trajectoryInitial}

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class search {

  def main(args: Array[String]): Unit = {

    val spark = new sparkInitial().start(args(0),s"tb-search data-${args(1).split("/").last.split("-").filter(!_.contains("20")).mkString("")} J-${args(2)} cores-${args(4)}",args(4),args(5))
    import spark.implicits._

    val tim = new Timer()

    val staTraOri01: RDD[Array[String]] = spark.sparkContext.textFile(s"${args(1)}").map(_.split("\t"))

    val (traGlobal1,indexParTH,cardSigAttrDF) = new trajectoryInitial().start(staTraOri01,spark,args(0),args(1),tim,true)


    traGlobal1.setName("traGlobal1")
    traGlobal1.persist()

    val sigS_par_Map = indexParTH.select($"sig_Seg", $"parTH").groupBy($"sig_Seg").agg(collect_set("parTH").as("sigSpars")). //sta_OD_F_ALL 每一个sta_OD_F都是一个乘客
      map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[mutable.WrappedArray[Int]].mkString(","))).collect().toMap

    //除了跑indexSize要取消注释，其他时候都注释上
//    val sigParRDD = indexParTH.select($"sig_Seg", $"parTH").groupBy($"sig_Seg").agg(collect_set("parTH").as("sigSpars")). //sta_OD_F_ALL 每一个sta_OD_F都是一个乘客
//      map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[mutable.WrappedArray[Int]].mkString(","))).rdd
//    sigParRDD.setName("sigParRDD")
//    sigParRDD.persist()

    val J = args(2).toDouble
    val Q_ARR = spark.read.format("parquet").load(s"${args(3)}").collect().map(x=>x(0).asInstanceOf[String])

    var allSimCount = 0; var searchTim = 0.0
    val allBuildTim = mutable.ArrayBuffer[Double]()
    var allQl = 0
    for(eachCard <- Q_ARR){
            val cardAttr = Q_BC.value
            val QH = cardAttr.drop(4).head.toInt
            val QS = cardAttr.drop(5).head.toInt
            val Ql = cardAttr.drop(6).head.toInt
            val QTra = cardAttr.head.split("#").drop(1)
            val minHS = new candiSig().minQTHS(QH, QS, c, d)
            val timeFor = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val QTiTra: Array[Long] = QTra.map(_.split("[.]").head).map(timeFor.parse(_).getTime)
            val TTiTra: Array[Long] = a.map(_.split("[.]").head).map(timeFor.parse(_).getTime)
            val c_li = new comTraFilter().li(QTiTra, TTiTra)
            if (!c_li) {
              val QidxF_TidxF_QidxL_TidxL = new comTraFilter().findFLTimePointInWindow(QTiTra, TTiTra)
              val allDiffTime = QidxF_TidxF_QidxL_TidxL._1 + QidxF_TidxF_QidxL_TidxL._2 + (Ql - QidxF_TidxF_QidxL_TidxL._3 + 1) + (b - QidxF_TidxF_QidxL_TidxL._4 + 1)
              val c_t_diff = allDiffTime > J * (Ql + b - 2 * minHS)
              !c_t_diff
            } else !c_li
          } else c_sig
        }
      searchResCan.setName("searchResCan")
      searchResCan.persist()
//      println(s"filter end candidate size is ${searchResCan.count}")

      val build = tim.elapsed()
//      println(s"build index and partition end ${args(1)} is $build(s)")

      allBuildTim += build

      tim.restart()

      val searchRes = searchResCan.map { can =>
        val QTraAttrArr = Q_BC.value
        val TTraAttrArr = can._2.split("###")
        if (!new refineCon().get(QTraAttrArr, TTraAttrArr, J)) {
          val QTra: Array[String] = QTraAttrArr.head.split("#").drop(1)
          val TTra: Array[String] = TTraAttrArr.head.split("#").drop(1)
          //20181101 08:16:18.石厦站.三号线.I, 20181101 08:32:06.农林站.七号线.O
          var x = 0;
          var y = 0;
          val Q_OD_tra = ArrayBuffer[String]();
          val T_OD_tra = ArrayBuffer[String]()
          while (x < QTra.length - 1) {
            Q_OD_tra += QTra.slice(x, x + 2).mkString("<>");
            x = x + 2 //  20181101 08:16:33.石厦站.三号线.I<>20181101 08:32:05.农林站.七号线.O,
          }
          while (y < TTra.length - 1) {
            T_OD_tra += TTra.slice(y, y + 2).mkString("<>");
            y = y + 2 //  20181101 08:16:18.石厦站.三号线.I<>20181101 08:32:06.农林站.七号线.O,
          }
          val D = new measure().Edr(Q_OD_tra.toArray, T_OD_tra.toArray)
          val EDR = D / (Q_OD_tra.length + T_OD_tra.length - D)
          (QTraAttrArr.mkString("###"), can._2, EDR)
        } else
        {
          (QTraAttrArr.mkString("###"), can._2, J)
        } //add T to QsimPairs  similarRes = similarRes.::(iter.next(), J )
      }.toDF("Q", "T", "J")
      val searchResJ = searchRes.filter($"J" <= J).filter($"Q" =!= $"T")
      val simSeaCount = searchResJ.count()
      allSimCount += simSeaCount.toInt
      val search = tim.elapsed()
      searchTim += search
    }
    println(s"*** cores = ${args(4)} ;\n *** J = ${args(2)}")
    println(s"query size is ${Q_ARR.length}. ave query tra len is ${allQl.toDouble/Q_ARR.length.toDouble}")
    println(s"all Sim Tra Count is ${allSimCount}")
    println(s"ave index building time is ${allBuildTim.head+(allBuildTim.drop(1).sum/(Q_ARR.length.toDouble-1))}(s)")
    println(s"ave search time is ${searchTim/Q_ARR.length.toDouble}(s)")

    spark.stop()
  }
}
