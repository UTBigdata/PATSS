package ours_LCPS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import ours_LCPS.opt.{candiSig, measure, refineCon, comTraFilter}
import ours_LCPS.tool.{Timer, attrGet, localPar, sparkInitial, trajectoryInitial}

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class search {

  def main(args: Array[String]): Unit = { //"384", dataOri, th.toString, dataOri+"-tb-0.8-join-query30", "192", master

    val spark = new sparkInitial().start(args(0),s"tb-search data-${args(1).split("/").last.split("-").filter(!_.contains("20")).mkString("")} J-${args(2)} cores-${args(4)}",args(4),args(5))
    import spark.implicits._

    val tim = new Timer()

    val staTraOri01: RDD[Array[String]] = spark.sparkContext.textFile(s"${args(1)}").map(_.split("\t"))

    val (traGlobal1,indexParTH,cardSigAttrDF) = new trajectoryInitial().start(staTraOri01,spark,args(0),args(1),tim, optPar = true)

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
//      252178676#20181103 10:38:53.兴东站.五号线.I#20181103 10:56:07.宝安中心站.一号线.O#20181103 12:34:53.宝安中心站.一号线.I#20181103 12:50:47.宝安站.十一号线.O#20181103 12:52:28.宝安站.十一号线.I#20181103 13:24:27.沙井站.十一号线.O#20181103 17:32:44.沙井站.十一号线.I#20181103 18:05:03.宝安站.十一号线.O#20181103 21:16:21.宝安中心站.一号线.I#20181103 21:33:16.兴东站.五号线.O#20181105 18:59:14.兴东站.五号线.I#20181105 19:12:27.宝安中心站.一号线.O#20181105 21:02:46.宝安中心站.一号线.I#20181105 21:14:49.兴东站.五号线.O#20181111 15:49:16.兴东站.五号线.I#20181111 16:04:25.宝安中心站.一号线.O#20181111 19:16:47.宝安中心站.一号线.I#20181111 19:29:17.兴东站.五号线.O#20181112 18:50:53.兴东站.五号线.I#20181112 19:03:09.宝安中心站.一号线.O#20181112 20:57:15.宝安中心站.一号线.I#20181112 21:14:48.兴东站.五号线.O#20181117 17:31:04.翻身站.五号线.I#20181117 17:37:38.宝安中心站.一号线.O#20181117 21:26:07.宝安中心站.一号线.I#20181117 21:40:04.兴东站.五号线.O#20181119 18:55:04.兴东站.五号线.I#20181119 19:06:43.宝安中心站.一号线.O#20181119 20:58:34.宝安中心站.一号线.I#20181119 21:13:48.兴东站.五号线.O#20181124 10:55:28.灵芝站.五号线.I#20181124 11:07:10.宝安中心站.一号线.O#20181124 18:32:10.兴东站.五号线.I#20181124 18:47:53.宝安中心站.一号线.O#20181124 21:16:28.宝安中心站.一号线.I#20181124 21:34:00.兴东站.五号线.O#20181126 18:58:18.兴东站.五号线.I#20181126 19:12:09.宝安中心站.一号线.O#20181126 20:56:48.宝安中心站.一号线.I#20181126 21:06:52.兴东站.五号线.O
//      ###一号线<>五号线-五号线<>一号线
//      ###宝安中心站<>坂田站-五和站-民治站-深圳北站-长岭陂站-塘朗站-大学城站-西丽站-留仙洞站-兴东站-洪浪北站[+]五号线<>一号线
//      ###宝安中心站<>兴东站-兴东站<>宝安中心站
//      ###8###7###40  CH CS L
      val cardAttr = eachCard.split("###")
      val Q_BC = spark.sparkContext.broadcast(cardAttr);
      val Q_sigSeg = cardAttr.drop(2).head
      val Q_sigSta = cardAttr.drop(3).head
      val QH = cardAttr.drop(4).head.toInt
      val QS = cardAttr.drop(5).head.toInt
      val Ql = cardAttr.drop(6).head.toInt
      allQl += Ql
      val QParTH: Array[Int] = new localPar().get(Q_sigSeg, sigS_par_Map).split(",").map(_.toInt)

      val relTra: RDD[(Int, String)] = traGlobal1.mapPartitionsWithIndex { (idx, iter) =>
        if (QParTH.contains(idx)) {
          iter
        } else {
          Iterator.empty
        }
      }.filter(row => row._2.nonEmpty).repartition(192)

      val Q_canSigArr = new candiSig().searchGet(Q_sigSeg, Ql, QH, QS, J) //sig两轮过滤  //sigSeg@Tl@TH@TS
      val q_canSigArrBC = spark.sparkContext.broadcast(Q_canSigArr)

      val searchResCan = relTra.map { T => //生成sig@Tl@TH@TS和TraAttr
          (T._2.split("###")(2) + "@" + T._2.split("###")(6) +
            "@" + T._2.split("###")(4) + "@" + T._2.split("###")(5),
            T._2)
        }.
        filter { can =>
          val canSig = q_canSigArrBC.value
          val c_sig = canSig.contains(can._1)
          if (c_sig) {
            val TTraAttrArr = can._2.split("###")
            val (a, b, c, d) = new attrGet().start(TTraAttrArr)   //(Tra,pl,H,S)
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
              val c_t_diff = allDiffTime > ((1 - J)/(1 + J)) * (Ql + b)
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
        if (!new refineCon().get(QTraAttrArr, TTraAttrArr, J)) { //不跳过验证
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
          val D = new measure().Dlcss(Q_OD_tra.toArray, T_OD_tra.toArray)
          val LCPS = D / (Q_OD_tra.length + T_OD_tra.length - D)
          (QTraAttrArr.mkString("###"), can._2, LCPS)
        } else //跳过验证，即一定大于J的
        {
          (QTraAttrArr.mkString("###"), can._2, J)
        } //add T to QsimPairs  similarRes = similarRes.::(iter.next(), J )
      }.toDF("Q", "T", "J")
      val searchResJ = searchRes.filter($"J" >= J).filter($"Q" =!= $"T")
      val simSeaCount = searchResJ.count()
      allSimCount += simSeaCount.toInt
      val search = tim.elapsed()
      searchTim += search
//      println(s"search result count is $simSeaCount")
//      println(s"search time with ${args(1)} is $search(s)")
//      searchResJ.show(simSeaCount.toInt)
    }
    println(s"*** cores = ${args(4)} ;\n *** J = ${args(2)}")
    println(s"query size is ${Q_ARR.length}. ave query tra len is ${allQl.toDouble/Q_ARR.length.toDouble}")
    println(s"all Sim Tra Count is ${allSimCount}")
    println(s"ave index building time is ${allBuildTim.head+(allBuildTim.drop(1).sum/(Q_ARR.length.toDouble-1))}(s)")
    println(s"ave search time is ${searchTim/Q_ARR.length.toDouble}(s)")

    spark.stop()
  }
}