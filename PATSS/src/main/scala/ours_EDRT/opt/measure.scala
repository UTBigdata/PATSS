package ours_EDRT.opt

import java.text.SimpleDateFormat

class measure {
  def phyODSim(ODq: String, ODt: String): Double = { //20181101 08:16:18.石厦站.三号线.I<>20181101 08:32:06.农林站.七号线.O
    val qO = ODq.split("<>")(0).split("[.]")
    val qOtime = qO(0)
    val qOStation = qO(1)
    val qOIO = qO(3)

    val qD = ODq.split("<>")(1).split("[.]")
    val qDtime = qD(0)
    val qDStation = qD(1)
    val qDIO = qD(3)

    val tO = ODt.split("<>")(0).split("[.]")
    val tOtime = tO(0)
    val tOStation = tO(1)
    val tOIO = tO(3)

    val tD = ODq.split("<>")(1).split("[.]")
    val tDtime = tD(0)
    val tDStation = tD(1)
    val tDIO = tD(3)

    var c1v = 0.0
    val c3 = qOIO.equals(tOIO) & qDIO.equals(tDIO);
    val c1 = qOStation.equals(tOStation) & qDStation.equals(tDStation);
    if (!(c1 & c3)) {
      c1v = 0.0
    } else c1v = 1.0

    var c2v = 0.0
    val time3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val c2 = math.abs(time3.parse(qOtime).getTime - time3.parse(tOtime).getTime) < 15 * 1000 * 60 && math.abs(time3.parse(qDtime).getTime - time3.parse(tDtime).getTime) < 15 * 1000 * 60;
    if (!c2) {
      c2v = 0.0
    } else c2v = 1.0

    (c1v + c2v) / 2
  }

  def Edr(str1: Array[String], str2: Array[String]): Double = {
    val matrix = Array.ofDim[Double](str1.length + 1, str2.length + 1)
    for (i <- 0 to str1.length;
         j <- 0 to str2.length) {

      if (i == 0) matrix(i)(j) = j
      else if (j == 0) matrix(i)(j) = i
      else matrix(i)(j) = min(matrix(i - 1)(j) + 1,
        matrix(i - 1)(j - 1) + (if (phyODSim(str1(i - 1), str2(j - 1)) == 1) 0 else if(phyODSim(str1(i - 1), str2(j - 1)) == 0.5) 0.5 else 1),
        matrix(i)(j - 1) + 1)
    }
    matrix(str1.length)(str2.length)
  }

  def min(num1: Double, num2: Double, num3: Double): Double = {
    if (num1 < num2) num1
    else if (num3 < num2) num3
    else num2
  }

}
