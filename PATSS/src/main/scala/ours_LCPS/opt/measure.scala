package ours_LCPS.opt

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

  def phyODSimSpa(ODq: String, ODt: String): Double = { //20181101 08:16:18.石厦站.三号线.I<>20181101 08:32:06.农林站.七号线.O
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
    if (!(c1)) { //& c3
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

  def phyPointSim(pq: String, pt: String): Boolean = {
    val q = pq.split("[.]")
    val qtime = q(0)
    val qStation = q(1)
    val qline = q(2)
    val qIO = q(3)

    val t = pt.split("[.]")
    val ttime = t(0)
    val tStation = t(1)
    val tline = t(2)
    val tIO = t(3)

    val c0 = qline.equals(tline);
    if (!c0) {
      return false
    }
    val c1 = qStation.equals(tStation);
    if (!c1) {
      return false
    }
    val time3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val c2 = math.abs(time3.parse(qtime).getTime - time3.parse(ttime).getTime) < 15 * 1000 * 60;
    if (!c2) {
      return false
    }
    val c3 = qIO.equals(tIO);
    if (!c3) {
      return false
    }
    true
  }

  def Dlcss(a: Array[String], b: Array[String]): Double = {
    var current: Array[Double] = new Array(b.length + 1)
    var previous = current
    for (i <- 1 to a.length) {
      previous = current
      for (j <- 1 to b.length) {
        if (phyODSim(a(i - 1), b(j - 1)) > 0)
          current(j) = phyODSim(a(i - 1), b(j - 1)) + previous(j - 1)
        else current(j) = math.max(previous(j), current(j - 1))
      }
    }
    current.last
  }

  def DlcssSpa(a: Array[String], b: Array[String]): Double = {
    var current: Array[Double] = new Array(b.length + 1)
    var previous = current
    for (i <- 1 to a.length) {
      previous = current
      for (j <- 1 to b.length) {
        if (phyODSimSpa(a(i - 1), b(j - 1)) > 0)
          current(j) = phyODSimSpa(a(i - 1), b(j - 1)) + previous(j - 1)
        else current(j) = math.max(previous(j), current(j - 1))
      }
    }
    current.last
  }

}
