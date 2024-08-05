package ours_LCPS.opt

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class candiSig {

  def minQTHS(QH:Int,QS:Int,TH:Int,TS:Int):Int = math.min(QH,TH)+math.min(QS,TS)
  def lBesidesC(l:Int,H:Int,S:Int):Int = if(l-2*H-2*S > 0) l-2*H-2*S else 0
  def minL_besides_C(Ql:Int,QH:Int,QS:Int,Tl:Int,TH:Int,TS:Int):Int = math.min(lBesidesC(Ql,QH,QS),lBesidesC(Tl,TH,TS))
  def interMax(Ql:Int,QH:Int,QS:Int,Tl:Int,TH:Int,TS:Int):Int = 2*minQTHS(QH,QS,TH,TS) + minL_besides_C(Ql,QH,QS,Tl,TH,TS)
  def unionMin(Ql:Int,QH:Int,QS:Int,Tl:Int,TH:Int,TS:Int):Int = Ql+Tl-interMax(Ql,QH,QS, Tl, TH, TS)
  def J_frac_inter_union(Ql:Int,QH:Int,QS:Int,Tl:Int,TH:Int,TS:Int):Double =
    interMax(Ql,QH,QS, Tl, TH, TS).toDouble/unionMin(Ql,QH,QS, Tl, TH, TS).toDouble

  def findPossibleHS(c: Int): Array[(Int, Int)] = { //c是THS_sum
    var result = Array[(Int, Int)]()
    for (a <- 1 to c) {
      for (b <- 1 to c - a) {
        if (a + b <= c) {
          result = result :+ (a, b)
        }
      }
    }
    result
  }

  def diff(a:Int,b:Int):Int = 2*math.abs(a - b)

  def diffSetMin(Ql:Int,QH:Int,QS:Int,Tl:Int,TH:Int,TS:Int) = diff(QH,TH) + diff(QS,TS) + diff(lBesidesC(Ql,QH,QS),lBesidesC(Tl,TH,TS)) / 2

  def searchGet(Q_sig:String,Ql:Int,QH:Int,QS:Int,J:Double):Array[String]={
    val candiSigArr = ArrayBuffer[String]()  //Q_sig@Tl@TH@TS
    val canLenRng: Seq[Int] = (Ql*J).ceil.toInt to (Ql/J).floor.toInt
    for(d <- canLenRng){
      val candiSumHS = (d/2.0).floor.toInt
      val canHS_combination: Array[(Int, Int)] = findPossibleHS(candiSumHS)
      for(x <- canHS_combination){
        val TH:Int = x._1 ; val TS:Int = x._2
        val J_max = J_frac_inter_union(Ql,QH,QS,d,TH,TS)
        val candiSig = Array(Q_sig,d,TH,TS).mkString("@")
        if(J_max >= J ){  //& diffSetMin(Ql,QH,QS,d,TH,TS) <= (1-J)*unionMax(Ql,QH,QS,d,TH,TS).toDouble
          candiSigArr += candiSig
        }
      }
    }
    candiSigArr.toArray
  }

  def oriFilterByCanCon(ori_lHS: Seq[String],Q_sigSeg:String,Q_sigSta:String,Ql: Int, QH: Int, QS: Int, J: Double): Array[String] = {
    val candiSigArr = ArrayBuffer[String]()
    for(lHS <- ori_lHS){
      val l_H_S = lHS.split("@")
      val d = l_H_S(0).toInt; val TH = l_H_S(1).toInt; val TS = l_H_S(2).toInt
      val candiSig = Array(Q_sigSeg, Q_sigSta, d, TH, TS).mkString("@")
      val c3 = d > Ql / J || d < Ql * J
      val J_max = J_frac_inter_union(Ql, QH, QS, d, TH, TS)
      val c1 = J_max < J;
      if (!(c1 || c3)) { // || c2
        candiSigArr += candiSig
      }
    }
    candiSigArr.toArray
  }

}

