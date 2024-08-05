package LCPS

import ours_LCPS.search
import ours_LCPS.tool.log

object searchBatch {
  def main(args: Array[String]): Unit = {

    new log().level()
    val master = args.last
    //new search().main()//args(parNum,oriData,J,queryPath,totalExeCores,Master)
    println("           ~~~~~~~~~~~~~~~~all SZT-Nov HZ-Jan tb search result~~~~~~~~~~~~~~~~             ")
    //threshold: 0.5 - 0.9
    //parNum: 384
    println("============================efficiency SZT-Nov & HZ-Jan LCSS tb search============================")
    for (dataOri <- Array("/ours/SZT/all/SZT-2018Nov-100", "/ours/HZ/all/HZ-2019Jan-100")) {
      println("——————————————————————————————————————————————————————————————————————")
      for (th <- Range(5, 10, 1).map(_ / 10.0)) {
        val args = Array("384", dataOri, th.toString, dataOri+"-tb-0.8-join-query30", "192", master)
        println("··································································")
        new search().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //dataSampleRate: 0.25 0.5 0.75 1.0
    //threshold: 0.8  parNum: 384
    println("============================scalability SZT-Nov & HZ-Jan LCSS tb search============================")
    val dataOriBaseArr = Array("/ours/SZT/all/SZT-2018Nov-", "/ours/HZ/all/HZ-2019Jan-")
    for (dataOriBase <- dataOriBaseArr) {
      println("——————————————————————————————————————————————————————————————————————")
      for (sr <- Range(25, 125, 25)) {
        val args = Array("384", dataOriBase + sr, "0.8", dataOriBase + "100-tb-0.8-join-query30" , "192", master)
        println("··································································")
        new search().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //coresNum 48-192
    //threshold 0.8
    println("============================scale up SZT-Nov & HZ-Jan LCSS tb search============================")
    for (dataOri <- Array("/ours/SZT/all/SZT-2018Nov-100", "/ours/HZ/all/HZ-2019Jan-100")) {
      println("——————————————————————————————————————————————————————————————————————")
      for (coresNum <- Range(48, 240, 16 * 3)) {
        val args = Array((coresNum * 2).toString, dataOri, "0.8", dataOri+"-tb-0.8-join-query30", coresNum.toString, master)
        println("··································································")
        new search().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //coresNum 48-192
    //threshold 0.8
    println("============================scale out SZT-Nov & HZ-Jan LCSS tb search============================")
    val dataCorePair: Array[Array[(String, Int)]] = Array(
      Array("/ours/SZT/all/SZT-2018Nov-25", "/ours/SZT/all/SZT-2018Nov-50", "/ours/SZT/all/SZT-2018Nov-75", "/ours/SZT/all/SZT-2018Nov-100"),
      Array("/ours/HZ/all/HZ-2019Jan-25", "/ours/HZ/all/HZ-2019Jan-50", "/ours/HZ/all/HZ-2019Jan-75", "/ours/HZ/all/HZ-2019Jan-100")
    ).map(_.zip(Range(48, 240, 16 * 3)))
    for (data <- dataCorePair) {
      println("——————————————————————————————————————————————————————————————————————")
      for (dataSR_core <- data) {
        val dataOri = dataSR_core._1
        val coresNum = dataSR_core._2
        val args = Array((coresNum * 2).toString, dataOri, "0.8", dataOri+"-tb-0.8-join-query30", coresNum.toString, master)
        println("··································································")
        new search().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }

  }
}
