package ours_LCPS.tool

class attrGet {
  def start(traAttr:Seq[String]):(Array[String],Int,Int,Int)={
    val Tra = traAttr.head.split("#").drop(1)
    val pl = traAttr(6).toInt
    val H = traAttr(4).toInt
    val S = traAttr(5).toInt
    (Tra,pl,H,S)
  }
}
