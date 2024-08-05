package ours_LCPS.opt

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

class heuristicPartition {

  def cross(a: mutable.Buffer[(String, Int)], b: mutable.Buffer[(String, Int)]): (mutable.Buffer[(String, Int)], mutable.Buffer[(String, Int)]) = {
    if (a.nonEmpty && b.nonEmpty) {
      val random = new Random
      val nA = random.nextInt(a.size)
      val nB = random.nextInt(b.size)
      val temp = a(nA)
      a(nA) = b(nB)
      b(nB) = temp
      (a, b)
    } else (a, b)
  }

  def evaluate(targetSum: Double, x: Seq[Int], size: Int): Int = {
    if (size != 1) {
      if (Range((targetSum * 0.9).floor.toInt, (targetSum * 1.1).ceil.toInt).contains(x.sum.toDouble)) 1 else 0
    }
    else {
      if (x.sum < (targetSum * 0.9).floor) 1 else 0
    }
  }

  def GA(waitCoIndex: ArrayBuffer[(String, Int)], eachParCount1: Double, k:Int, hzmini25:Boolean): Array[String] = {
    val eachParCount = if(hzmini25) eachParCount1 else eachParCount1.ceil
    waitCoIndex.foreach { x: (String, Int) =>
      if (x._2 > eachParCount * 1.1)
        waitCoIndex(waitCoIndex.indexOf(x)) = (waitCoIndex(waitCoIndex.indexOf(x))._1,eachParCount.toInt)
    }
    val M: mutable.Seq[Int] = waitCoIndex.map(_._2)
//    println(M.mkString(","))
    val sumM = M.sum
    val meanN = sumM.toDouble / M.length.toDouble
    val DimN = eachParCount.toDouble / meanN
    val meanM = meanN
    val x = M.count(_ <= meanM).toDouble / M.length.toDouble
    val DimM = ((x / 0.5) * DimN).ceil.toInt //理论平均维度
//    val k = (sumM.toDouble / eachParCount.toDouble).ceil.toInt
//    println(s"k is === $k")
//    println(s"eachParCount === $eachParCount")
//    val k = BigDecimal(k0).setScale(0, BigDecimal.RoundingMode.HALF_UP).toInt
//    if(fff) k = k+1
//    println(M.length,M.sum,DimM,k)
    val round3 = 500;
    val round2 = 20;
    val round1 = 1000

    var C_N_Final = ListBuffer[mutable.Buffer[String]]()
    val r1 = 0;
    var flag2 = true
    while (r1 < round1 && flag2) { //repss 10
      var len = M.size
      val C_N = ListBuffer[mutable.Buffer[String]]()
      var Npop = ListBuffer[(String, Int)]()
      val pop = ListBuffer[mutable.Buffer[(String, Int)]]()
      var new_pop = ListBuffer[mutable.Buffer[(String, Int)]]()

      //initial
      while (len != 0) {
        if (len < DimM + 1) {
          val n = len
          val DAnew = waitCoIndex.slice(len - n, len)
          pop += DAnew
          len -= n
        }
        else if (len == DimM + 1) {
          val n = (DimM + 1) / 2
          val DAnew = waitCoIndex.slice(len - n, len)
          pop += DAnew
          len -= n
        }
        else {
          val n = ((1 - x) * DimM).toInt + new Random().nextInt(DimM + 1)
          val DAnew = waitCoIndex.slice(len - n, len)
          pop += DAnew
          len -= n
        }
      }

      var flag1 = true
      var r2 = 0

      while (r2 < round2 && flag1) {
        if (r2 == 0) new_pop = pop
        else if (r2 > 0) {
          var len = Npop.length
          val Dim = (eachParCount / (Npop.map(_._2).sum / Npop.size)).ceil.toInt
          val NNpop = ListBuffer[mutable.Buffer[(String, Int)]]()
          while (len != 0) { //个体初始化
            if (len < Dim + 1) {
              val n = len
              val DAnew = Npop.slice(len - n, len)
              NNpop += DAnew
              len -= n
            }
            else if (len == Dim + 1) {
              val n = (Dim + 1) / 2
              val DAnew = Npop.slice(len - n, len)
              NNpop += DAnew
              len -= n
            }
            else {
              var n = 0
              if (Dim - 1 == 0) {
                n = 2
              }
              else {
                n = ((1 - x) * Dim).toInt + new Random().nextInt(Dim + 1)
              }
              val DAnew = Npop.slice(len - n, len)
              NNpop += DAnew
              len -= n
            }
          }
          new_pop = NNpop
        }

        var r3 = 0;
        var flag = true
        while (r3 < round3 && flag) {
          val C_M = ListBuffer[Int]()
          for (i <- new_pop.indices) {
            val x = evaluate(eachParCount, new_pop(i).map(_._2), new_pop.size)
            if (x == 1) {
              C_N += new_pop(i).map(_._1)
              C_M += i + 1
            }
          }
          for (i <- C_M.indices) new_pop = new_pop.slice(0, C_M(i) - 1 - i) ++ new_pop.slice(C_M(i) - i, new_pop.size)
          if (new_pop.size == 1 || new_pop.isEmpty) flag = false
          if (flag) {
            for (i <- new_pop.indices) {
              val m = new Random().nextInt(new_pop.size)
              if (i != m) {
                cross(new_pop(i), new_pop(m))
              }
            }
          }
          r3 += 1
        }
        if (new_pop.size == 1 || new_pop.isEmpty) {
          flag1 = false
        }
        Npop.drop(Npop.size)
        Npop = new_pop.flatten
        r2 += 1
      }

      if (new_pop.isEmpty && C_N.size == k) {
        println("Match Success"); flag2 = false; C_N_Final = C_N
      }
      else if (new_pop.length == 1 && C_N.size == k - 1) {
        if (new_pop.head.map(_._2).sum < eachParCount) {
          println("Match Success");
          flag2 = false
          C_N_Final = C_N.+=(new_pop.head.map(_._1))
        }
        else {
          print("*"); C_N_Final += mutable.Buffer("Failure")
        }
      }
      else {
        print("*"); C_N_Final += mutable.Buffer("Failure")//println("match failure")
      }
    }
    C_N_Final.map(_.toArray.mkString("&")).toArray
  }

}
