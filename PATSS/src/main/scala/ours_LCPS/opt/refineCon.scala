package ours_LCPS.opt

import java.text.SimpleDateFormat
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class refineCon {
  private def ODSim(ODq: String, ODt: String): Boolean = { //20181101 08:16:18.石厦站.三号线.I<>20181101 08:32:06.农林站.七号线.O
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

    val tD = ODt.split("<>")(1).split("[.]")
    val tDtime = tD(0)
    val tDStation = tD(1)
    val tDIO = tD(3)

    val c1 = qOStation.equals(tOStation) & qDStation.equals(tDStation);
    if (!c1) {
      return false
    }
    val time3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val c2 = math.abs(time3.parse(qOtime).getTime - time3.parse(tOtime).getTime) < 15 * 1000 * 60 && math.abs(time3.parse(qDtime).getTime - time3.parse(tDtime).getTime) < 15 * 1000 * 60;
    if (!c2) {
      return false
    }
    val c3 = qOIO.equals(tOIO) & qDIO.equals(tDIO);
    if (!c3) {
      return false
    }
    true
  }
  def get(Q: Seq[String], T: Seq[String], J: Double): Boolean = {
    val QHSODsta = Q(3) //赤尾站<>华强北站-华强北站<>赤尾站
    val QTra = Q.head.split("#").drop(1); //(20181101 08:16:33.石厦站.三号线.I, 20181101 08:32:05.农林站.七号线.O,
    val QCH = Q(4).toInt;
    val QCS = Q(5).toInt;
    val Qpl = Q(6).toInt //三号线<>三号线-三号线<>三号线  //六约站-塘坑站<>老街站[+]三号线<>三号线
    val THSODsta = T(3) //赤尾站<>华强北站-华强北站<>赤尾站
    val TTra = T.head.split("#").drop(1);
    val TCH = T(4).toInt;
    val TCS = T(5).toInt;
    val Tpl = T(6).toInt
    val CminHS = math.min(QCH, TCH) + math.min(QCS, TCS)
    val cSpa = 2 * CminHS >= (J / (1 + J)) * (Qpl + Tpl)
    if (!cSpa) {
      return false
    }

    var x = 0;
    var y = 0;
    val Q_OD_tra = ArrayBuffer[String]()
    val T_OD_tra = ArrayBuffer[String]()
    while (x < QTra.length - 1) {
      Q_OD_tra += QTra.slice(x, x + 2).mkString("<>");
      x = x + 2 //  20181101 08:16:33.石厦站.三号线.I<>20181101 08:32:05.农林站.七号线.O,
    }
    val Q_OD_tra1 = Q_OD_tra.filter(x =>
      (x.contains(QHSODsta.split("-")(0).split("<>")(0)) & x.contains(QHSODsta.split("-")(0).split("<>")(1))) ||
        (x.contains(QHSODsta.split("-")(1).split("<>")(0)) & x.contains(QHSODsta.split("-")(1).split("<>")(1)))
    )
    while (y < TTra.length - 1) {
      T_OD_tra += TTra.slice(y, y + 2).mkString("<>");
      y = y + 2 //  20181101 08:16:18.石厦站.三号线.I<>20181101 08:32:06.农林站.七号线.O,
    }
    val T_OD_tra1 = T_OD_tra.filter(x =>
      (x.contains(THSODsta.split("-")(0).split("<>")(0)) & x.contains(THSODsta.split("-")(0).split("<>")(1))) ||
        (x.contains(THSODsta.split("-")(1).split("<>")(0)) & x.contains(THSODsta.split("-")(1).split("<>")(1)))
    )
    var interOD = ListBuffer[String]()
    for (q <- Q_OD_tra1) {
      for (t <- T_OD_tra1) {
        if (ODSim(q, t)) {
          interOD.append(t)
        }
      }
    }
    interOD = interOD.distinct
    val cTim = interOD.length >= CminHS;
    if (!cTim) {
      return false
    }
    cSpa & cTim
  }

  def getSpa(Q: Seq[String], T: Seq[String], J: Double): Boolean = {
    val QHSODsta = Q(3) //赤尾站<>华强北站-华强北站<>赤尾站
    val QTra = Q.head.split("#").drop(1); //(20181101 08:16:33.石厦站.三号线.I, 20181101 08:32:05.农林站.七号线.O,
    val QCH = Q(4).toInt;
    val QCS = Q(5).toInt;
    val Qpl = Q(6).toInt //三号线<>三号线-三号线<>三号线  //六约站-塘坑站<>老街站[+]三号线<>三号线
    val THSODsta = T(3) //赤尾站<>华强北站-华强北站<>赤尾站
    val TTra = T.head.split("#").drop(1);
    val TCH = T(4).toInt;
    val TCS = T(5).toInt;
    val Tpl = T(6).toInt
    val CminHS = math.min(QCH, TCH) + math.min(QCS, TCS)
    val cSpa = 2 * CminHS >= (J / (1 + J)) * (Qpl + Tpl)
    if (!cSpa) {
      return false
    } //true为空间相似性通过，继而继续验证时间相似性。而false为逐一计算
    cSpa
  }
}
