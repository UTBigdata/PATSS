package ours_LCPS.tool

class localPar {
  def get(indexParTH: String, indexParsMap: Map[String, String]): String = {
    indexParsMap.getOrElse(indexParTH, "99999999")
  }
}
