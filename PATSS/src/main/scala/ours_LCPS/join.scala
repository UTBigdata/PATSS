package ours_LCPS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{array, collect_set, concat_ws, explode}
import ours_LCPS.opt.{candiSig, measure, refineCon, comTraFilter}
import ours_LCPS.tool._

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class join {
  def main(args: Array[String]): Unit = {   //"384" "/global_staTroOri0_sucReal" "0.6" "192" master true(是否是hzmini25)

    val spark = new sparkInitial().start(args(0), s"tb-join data-${args(1).split("/").last.split("-").filter(!_.contains("20")).mkString("")} J-${args(3)} cores-${args(2)}", args(3), args(4))

    import spark.implicits._

    //    val args = Array("384","/ours/Tra25Raw","0.6")

    val tim = new Timer()

    val staTraOri01: RDD[Array[String]] = spark.sparkContext.textFile(s"${args(1)}").map(_.split("\t"))

    val optPar = !args(1).contains("mini")

    val (traGlobal1,indexParTH,cardSigAttrDF) = new trajectoryInitial().
      start(staTraOri01 = staTraOri01, spark = spark, parNum = args(0), path = args(1), tim, optPar = optPar)

    val oriSigTraSet = cardSigAttrDF.select(
      $"seg_OD_F_S",
      $"sta_OD_F_S",
      concat_ws("@",$"seg_OD_F_S",$"sta_OD_F_S",$"pointTraLen",$"CH",$"CS"),
      concat_ws("@",$"pointTraLen",$"CH",$"CS"),
      concat_ws("###", $"card#pointTra", $"line_OD_F_S", $"seg_OD_F_S", $"sta_OD_F_S", $"CH", $"CS",$"pointTraLen")
      ).toDF(
      "sigSeg",
      "sigSta",
      "sigOpt",
      "lHS",
      "tra")

    //除了跑indexSize要取消注释，其他时候都注释上
//    val aa = oriSigTraSet.rdd
//    aa.setName("oriSigTraSet")
//    aa.persist()

    oriSigTraSet.persist()

    val sig_mapOfLhsTra = oriSigTraSet.groupBy($"sigSeg", $"sigSta", $"lHS").agg(collect_set($"tra").as("traSet")).
      groupBy($"sigSeg", $"sigSta").agg(collect_set(concat_ws("__", $"lHS", $"traSet")).as("lHS__tra__tra")).
      groupBy($"sigSeg").agg(collect_set(concat_ws("___", $"sigSta", $"lHS__tra__tra")).as("sigSta___lHS__tra__tra")).
      map { x =>
        val sigSeg = x(0).asInstanceOf[String];
        val sigSta_lHStratra = x(1).asInstanceOf[Seq[String]]
        val mmap = new mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()
        for (sigStalhstra <- sigSta_lHStratra) {
          val sigSta = sigStalhstra.split("___").head
          val lhstra: Array[String] = sigStalhstra.split("___").drop(1)
          val map = new mutable.HashMap[String, Seq[String]]()
          for (eachlsh <- lhstra) {
            val lsh = eachlsh.split("__").head
            val tras = eachlsh.split("__").drop(1)
            map.put(lsh, tras)
          }
          mmap.put(sigSta, map)
        }
        (sigSeg, mmap)
      } //.toDF("sigSeg","mapSigSta_mapLhs_Tra")
    //    sig_mapOfLhsTra.show(10)
    val oriSigTraSetCo = new mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]]()
    sig_mapOfLhsTra.
      map(x=>(x._1.asInstanceOf[String],x._2.asInstanceOf[mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]])).
      rdd.collect().foreach(x => oriSigTraSetCo.put(x._1, x._2))
    val oriSigTraSetBC = spark.sparkContext.broadcast(oriSigTraSetCo)

    val oriSig = oriSigTraSet.select($"sigOpt").distinct()
    val J = args(2).toDouble

    val build = tim.elapsed()
    println(s"build index and partition is $build(s)")

    tim.restart()

    oriSigTraSet.unpersist()

    val sigCanPair = oriSig.map{ x=>
      val attr = x(0).asInstanceOf[String].split("@")
      val Q_sigSeg = attr.head; val Q_sigSta = attr.drop(1).head ;val Ql = attr.drop(2).head.toInt; val QH = attr.drop(3).head.toInt; val QS = attr.last.toInt
      val ori_lHS: Seq[String] = oriSigTraSetBC.value(Q_sigSeg)(Q_sigSta).keys.toList
      val canSigS: Array[String] = new candiSig().oriFilterByCanCon(ori_lHS,Q_sigSeg,Q_sigSta,Ql,QH,QS,J)
      (x(0).asInstanceOf[String],canSigS)//两个都含有sigSeg@sigSta@l@H@S
    }.toDF("oriSig","canSigSet").filter(x=>x(1).asInstanceOf[Seq[String]].nonEmpty).
      select($"oriSig",explode($"canSigSet").as("canSig")).
      map{x=>
      val oriSig = x(0).asInstanceOf[String]; val oriHC = oriSig.hashCode
      val canSig = x(1).asInstanceOf[String]; val canHC = canSig.hashCode
      if(oriHC >= canHC) (canSig,oriSig) else (oriSig,canSig)
    }.distinct().rdd
    println(s"cores = ${args(3)} ; J = ${args(2)}")


    val traCanPair = sigCanPair.map{oriCan=>
      val oriSigTraSet = oriSigTraSetBC.value
      (
        oriSigTraSet(oriCan._1.split("@").head)(oriCan._1.split("@").drop(1).head).get(oriCan._1.split("@").drop(2).mkString("@")),
        oriSigTraSet(oriCan._2.split("@").head)(oriCan._2.split("@").drop(1).head).get(oriCan._2.split("@").drop(2).mkString("@"))
      )
    }.toDF("oriTraSet","canTraSet")
      .select(explode($"oriTraSet").as("oriTra"),$"canTraSet")
      .select($"oriTra",explode($"canTraSet").as("canTra"))
      .map{oriCanTraPair=>
        val oriTra = oriCanTraPair(0).asInstanceOf[String];val oriCard = oriTra.split("#").head.hashCode.toLong
        val canTra = oriCanTraPair(1).asInstanceOf[String];val canCard = canTra.split("#").head.hashCode.toLong
        if(oriCard < canCard) (oriTra,canTra) else (canTra,oriTra)
      }.distinct()
      .filter {oriCanTra=>
        val Q = oriCanTra._1 ; val T = oriCanTra._2
        val QTraAttrArr = Q.split("###")
        val TTraAttrArr = T.split("###")
        val (e, f, g, h) = new attrGet().start(QTraAttrArr)
        val (a, b, c, d) = new attrGet().start(TTraAttrArr)
        val minHS = new candiSig().minQTHS(g, h, c, d)
        val timeFor = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val QTiTra: Array[Long] = e.map(_.split("[.]").head).map(timeFor.parse(_).getTime)
        val TTiTra: Array[Long] = a.map(_.split("[.]").head).map(timeFor.parse(_).getTime)
        val c_li = new comTraFilter().li(QTiTra, TTiTra)
        if (!c_li) {
          val QidxF_TidxF_QidxL_TidxL = new comTraFilter().findFLTimePointInWindow(QTiTra, TTiTra)
          val allDiffTime = QidxF_TidxF_QidxL_TidxL._1 + QidxF_TidxF_QidxL_TidxL._2 + (f - QidxF_TidxF_QidxL_TidxL._3 + 1) + (b - QidxF_TidxF_QidxL_TidxL._4 + 1)
          val c_t_diff = allDiffTime > (1 - J) * (f + b - 2 * minHS);
          !c_t_diff
        } else !c_li
      }
//    sigCanPair.unpersist()
//    traCanPair.persist()
//    println(s"filter end | candidate/traCanPair size is ${traCanPair.count}")
//
    val joinRes = traCanPair.map{oriCan=>
      val QTraAttrArr = oriCan._1.split("###")
      val TTraAttrArr = oriCan._2.split("###")
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
        (oriCan._1, oriCan._2, LCPS)
      } else //跳过验证，即一定大于J的
      {
        (oriCan._1, oriCan._2, J)
      } //add T to QsimPairs  similarRes = similarRes.::(iter.next(), J )
    }.toDF("Q", "T", "J")
    val searchResJ = joinRes.filter($"J" >= J && $"Q" =!= $"T")
    val simJoinCount = searchResJ.count()

    val join = tim.elapsed()
    println(s"join time is $join(s)")
    println(s"join result count is $simJoinCount")
//    searchResJ.write.format("parquet").mode("overwrite").save(s"${args(1)}-tb-${args(2)}-join")

    spark.stop()
  }

}
