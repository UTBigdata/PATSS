package ours_EDRT.opt

import scala.util.control.Breaks.{break, breakable}

class comTraFilter {
  def li(QTiTra: Array[Long], TTiTra: Array[Long]):Boolean={
    val QTiF = QTiTra.head;
    val QTiL = QTiTra.last;
    val TTiF = TTiTra.head;
    val TTiL = TTiTra.last;
    QTiL < TTiF || TTiL < QTiF;
  }
  def findFLTimePointInWindow(QTiTra: Array[Long], TTiTra: Array[Long]): (Int, Int, Int, Int) = {
    var short = Array[Long]();
    var long = Array[Long]()
    if (QTiTra.length < TTiTra.length) {
      short = QTiTra;
      long = TTiTra
    } else {
      short = TTiTra;
      long = QTiTra
    }
    var QidxF = -1;
    var QidxL = -1;
    var TidxF = -1;
    var TidxL = -1
    var flag = false

    breakable {
      for (timeQ <- QTiTra) {
        for (timeT <- TTiTra) {
          if (math.abs(timeQ - timeT) / 1000 / 60 < 15) {
            QidxF = QTiTra.indexOf(timeQ)
            TidxF = TTiTra.indexOf(timeT)
            break
            flag = true
          }
        }
        if (flag) break
      }
    }

    breakable {
      for (timeQ <- QTiTra.reverse) {
        for (timeT <- TTiTra.reverse) {
          if (math.abs(timeQ - timeT) / 1000 / 60 < 15) {
            QidxL = QTiTra.indexOf(timeQ)
            TidxL = TTiTra.indexOf(timeT)
            break
            flag = true
          }
        }
        if (flag) break
      }
    }

    (QidxF, TidxF, QidxL, TidxL)
  }

  def findFLSpaPoint(QTiTra: Array[String], TTiTra: Array[String]): (Int, Int, Int, Int) = {
    var short = Array[String]();
    var long = Array[String]()
    if (QTiTra.length < TTiTra.length) {
      short = QTiTra;
      long = TTiTra
    } else {
      short = TTiTra;
      long = QTiTra
    }
    var QidxF = -1;
    var QidxL = -1;
    var TidxF = -1;
    var TidxL = -1
    var flag = false

    breakable {
      for (timeQ <- QTiTra) {
        for (timeT <- TTiTra) {
          if (timeQ.equals(timeT)) {
            QidxF = QTiTra.indexOf(timeQ)
            TidxF = TTiTra.indexOf(timeT)
            break
            flag = true
          }
        }
        if (flag) break
      }
    }

    breakable {
      for (timeQ <- QTiTra.reverse) {
        for (timeT <- TTiTra.reverse) {
          if (timeQ.equals(timeT)) {
            QidxL = QTiTra.indexOf(timeQ)
            TidxL = TTiTra.indexOf(timeT)
            break
            flag = true
          }
        }
        if (flag) break
      }
    }

    (QidxF, TidxF, QidxL, TidxL)
  }

}
