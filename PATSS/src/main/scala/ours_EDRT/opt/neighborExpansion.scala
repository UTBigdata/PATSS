package ours_EDRT.opt

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class neighborExpansion {
  def ff(matrix: Array[Array[Int]]): Unit = {
    for (i <- matrix.indices) {
      for (j <- matrix(0).indices) {
        print("%4d ".format(matrix(i)(j)))
      }
      println()
    }
  } //Format output for 2D data

  def splitArr(rowsSplitRange: mutable.Buffer[Int], rows: Int): Array[Array[Int]] = {
    val ress = mutable.Buffer[Array[Int]]()
    val res = mutable.Buffer[Int]()
    for (i <- rowsSplitRange.indices) {
      if (i == 0) {
        res += rowsSplitRange(i)
      } else {
        if (rowsSplitRange(i) - rowsSplitRange(i - 1) == 1) {
          res += rowsSplitRange(i)
        } else {
          ress += res.toArray
          res.clear()
          res += rowsSplitRange(i)
        }
      }
    }
    ress += res.toArray
    ress.toArray
  }

  def splitLocal(x: Array[Int]): Array[Array[Int]] = {
    val ress1 = new mutable.ArrayBuffer[Array[Int]]()
    val flag = x.length / 2
    val y = x.slice(0, flag);
    val z = x.slice(flag, x.length)
    ress1.+=(y, z).sortWith(_.head < _.head).toArray
  }

  def splitMat(dimStaHOD: Array[Array[Int]], eachParCount: BigDecimal, LHOsta: Array[String], LHDsta: Array[String]): (Array[String], Array[String], Array[Array[Int]]) = {
    val rows = dimStaHOD.length
    val cols = dimStaHOD(0).length
    val dimStaHODtrn = dimStaHOD.flatten
    val EleMaxs0: Array[Int] = dimStaHODtrn.sorted.reverse.take(((math.sqrt
    ((dimStaHODtrn.sum.toDouble / eachParCount.toDouble).floor.toInt.ceil.toInt) - 1) / 2).ceil.toInt)

    val EleMaxs: Array[Int] = if (EleMaxs0.length == 0) {
      dimStaHODtrn.sorted.reverse.take(1)
    } else EleMaxs0

    val IndexArr2Dim = new ArrayBuffer[(Int, Int)](EleMaxs.length)
    for (i <- EleMaxs) {
      val Index = dimStaHODtrn.indexOf(i)
      var row = 0;
      var col = 0
      if (Index+1 % cols == 0) {
        col = cols - 1; row = Index / cols - 1
      } else {
        col = Index % cols; row = Index / cols
      }
      IndexArr2Dim.append((row, col))
    }

    val rowsFlag = IndexArr2Dim.map(_._1).distinct.sorted
    val rowsSplitRange = Range(0, rows).toBuffer

    rowsFlag.foreach(x => rowsSplitRange -= x)
    val rowsRanges = splitArr(rowsSplitRange, rows)
    var rowRangesL: Array[Array[Int]] = rowsRanges.union(rowsFlag.map(x => Array(x))).sortWith(_.head < _.head)

    val colsFlag = IndexArr2Dim.map(_._2).distinct.sorted
    val colsSplitRange = Range(0, cols).toBuffer

    colsFlag.foreach(x => colsSplitRange -= x)
    val colsRanges = splitArr(colsSplitRange, cols)
    var colRangesL = colsRanges.union(colsFlag.map(x => Array(x))).sortWith(_.head < _.head)

    var splitMatrix = Array.ofDim[Int](rowRangesL.length, colRangesL.length)
    for (rowSeg <- rowRangesL) {
      for (i <- rowSeg) {
        for (colSeg <- colRangesL) {
          for (j <- colSeg) {
            splitMatrix(rowRangesL.indexOf(rowSeg))(colRangesL.indexOf(colSeg)) += dimStaHOD(i)(j)
          }
        }
      }
    }

    ff(splitMatrix)
    val thdPar = eachParCount * 1.1
    var ffff = true
    while (splitMatrix.flatten.max > thdPar & ffff) {
      var colBuf = colRangesL.toBuffer
      var rowBuf = rowRangesL.toBuffer
      val colEmy = new ArrayBuffer[Array[Int]]()
      val rowEmy = new ArrayBuffer[Array[Int]]()
      for (elem <- splitMatrix) { //row
        for (ele <- elem) { //col
          if (ele > thdPar) {
            val idxC: Int = elem.indexOf(ele) //col
            val idxR: Int = splitMatrix.indexOf(elem) //row
            val colWaitSpt: Array[Int] = colRangesL(idxC)
            val rowWaitSpt: Array[Int] = rowRangesL(idxR)
            if (colWaitSpt.length == 1 && rowWaitSpt.length == 1) {
              dimStaHOD(rowWaitSpt.head)(colWaitSpt.head) = (thdPar / 1.1).toInt
            }
            if (colWaitSpt.length >= 2) {
              colEmy.append(colWaitSpt)
            }
            if (rowWaitSpt.length >= 2) {
              rowEmy.append(rowWaitSpt)
            }
          }
        }
      }
      colEmy.distinct.foreach { x =>
        colBuf -= x
        for (elem <- splitLocal(x)) {
          colBuf += elem
        }
      }

      rowEmy.distinct.foreach { x =>
        rowBuf -= x
        for (elem <- splitLocal(x)) {
          rowBuf += elem
        }
      }

      val splitMatrixPlus = Array.ofDim[Int](rowBuf.length, colBuf.length)
      rowBuf = rowBuf.sortWith(_.head < _.head)
      colBuf = colBuf.sortWith(_.head < _.head)
      for (rowSeg <- rowBuf) {
        for (i <- rowSeg) {
          for (colSeg <- colBuf) {
            for (j <- colSeg) {
              splitMatrixPlus(rowBuf.indexOf(rowSeg))(colBuf.indexOf(colSeg)) += dimStaHOD(i)(j)
            }
          }
        }
      }

      splitMatrix = splitMatrixPlus
      rowRangesL = rowBuf.sortWith(_.head < _.head).toArray
      colRangesL = colBuf.sortWith(_.head < _.head).toArray
      ff(splitMatrix)
    }

    ff(splitMatrix)
    val rowsStaRanges = new ArrayBuffer[Array[String]]()
    rowRangesL.sortWith(_.head < _.head).foreach(x => rowsStaRanges += LHOsta.slice(x.head, x.last + 1))
    val colsStaRanges = new ArrayBuffer[Array[String]]()
    colRangesL.sortWith(_.head < _.head).foreach(x => colsStaRanges += LHDsta.slice(x.head, x.last + 1))

    (rowsStaRanges.toArray.map(_.mkString("-")), colsStaRanges.toArray.map(_.mkString("-")), splitMatrix)

  }
}
