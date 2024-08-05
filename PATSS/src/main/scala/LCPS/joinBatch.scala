package LCPS

import ours_LCPS.join
import ours_LCPS.tool.log

object joinBatch {
  def main(args: Array[String]): Unit = {

    new log().level()
    val master = args.last

    //new join().main()//args(parNum,oriData,J,totalExeCores,Master)
    println("============================all SZT-Nov HZ-Jan tb join result============================")
    //threshold: 0.5 - 0.9
    //parNum: 384
    println("----------------------------efficiency SZT-Nov & HZ LCSS tb join for all----------------------------")
    for(dataOri <- Array("/ours/SZT/all/SZT-2018Nov-100", "/ours/HZ/all/HZ-2019Jan-100")){
      println("——————————————————————————————————————————————————————————————————————")
      for (th <- Range(5, 10, 1).map(_ / 10.0)) {
        val args = Array("384", dataOri, th.toString, "192", master)
        println("··································································")
        new join().main(args)  //output:/ours/HZ/all/HZ-2019Jan-100-tb-0.7-join
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //dataSampleRate: 0.25 0.5 0.75 1.0
    //threshold: 0.8  parNum: 384
    println("----------------------------scalability SZT-Nov & HZ LCSS tb join for all----------------------------")
    val dataOriBaseArr = Array("/ours/SZT/all/SZT-2018Nov-", "/ours/HZ/all/HZ-2019Jan-")
    for(dataOriBase <- dataOriBaseArr) {
      println("——————————————————————————————————————————————————————————————————————")
      for (sr <- Range(25, 125, 25)) {
        val args = Array("384", dataOriBase+sr, "0.8", "192", master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //coresNum 48-192 parNum
    //threshold 0.8
    println("----------------------------scale up SZT-Nov & HZ LCSS tb join for all----------------------------")
    for(dataOri <- Array("/ours/SZT/all/SZT-2018Nov-100", "/ours/HZ/all/HZ-2019Jan-100")){
      println("——————————————————————————————————————————————————————————————————————")
      for (coresNum <- Range(48, 240, 16*3)) {
        val args = Array((coresNum*2).toString, dataOri, "0.8", coresNum.toString, master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //coresNum 48-192
    //threshold 0.8
    println("----------------------------scale out SZT-Nov & HZ LCSS tb join for all----------------------------")
    val dataCorePair: Array[Array[(String, Int)]] = Array(
      Array("/ours/SZT/all/SZT-2018Nov-25", "/ours/SZT/all/SZT-2018Nov-50", "/ours/SZT/all/SZT-2018Nov-75", "/ours/SZT/all/SZT-2018Nov-100"),
      Array("/ours/HZ/all/HZ-2019Jan-25", "/ours/HZ/all/HZ-2019Jan-50", "/ours/HZ/all/HZ-2019Jan-75", "/ours/HZ/all/HZ-2019Jan-100")
    ).map(_.zip(Range(48, 240, 16 * 3)))
    for(data <- dataCorePair){
      println("——————————————————————————————————————————————————————————————————————")
      for(dataSR_core <- data){
        val dataOri = dataSR_core._1
        val coresNum = dataSR_core._2
        val args = Array((coresNum * 2).toString, dataOri, "0.8", coresNum.toString, master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }

    println("============================compare DITA SZT-Nov-mini(size 50000) HZ-Jan-mini(size 50000) tb join result============================")
    //threshold: 0.5 - 0.9
    //parNum: 384
    println("----------------------------efficiency SZT-Nov-mini & HZ-Jan-mini LCSS tb join for DITA----------------------------")
    for (dataOri <- Array("/ours/SZT/all/SZT-2018Nov-mini-100", "/ours/HZ/all/HZ-2019Jan-mini-100")) {
      println("——————————————————————————————————————————————————————————————————————")
      for (th <- Range(5, 10, 1).map(_ / 10.0)) {
        val args = Array("384", dataOri, th.toString, "192", master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //dataSampleRate: 0.25 0.5 0.75 1.0
    //threshold: 0.8  parNum: 384
    println("----------------------------scalability SZT-Nov-mini & HZ-Jan-mini LCSS tb join for DITA----------------------------")
    val dataOriMinBaseArr = Array("/ours/SZT/all/SZT-2018Nov-mini-", "/ours/HZ/all/HZ-2019Jan-mini-")
    for (dataOriBase <- dataOriMinBaseArr) {
      println("——————————————————————————————————————————————————————————————————————")
      for (sr <- Range(25, 125, 25)) {
        val args = Array("384", dataOriBase + sr, "0.8", "192", master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //coresNum 48 96 144 192 parNum:_*2
    //threshold 0.8
    println("----------------------------scale up SZT-Nov-mini & HZ-Jan-mini LCSS tb join for DITA----------------------------")
    for (dataOri <- Array("/ours/SZT/all/SZT-2018Nov-mini-100", "/ours/HZ/all/HZ-2019Jan-mini-100")) {
      println("——————————————————————————————————————————————————————————————————————")
      for (coresNum <- Range(48, 240, 16 * 3)) {
        val args = Array((coresNum * 2).toString, dataOri, "0.8", coresNum.toString, master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }
    //coresNum 48-192
    //threshold 0.8
    println("----------------------------scale out SZT-Nov-L22 & HZ-Jan-L LCSS tb join for DITA----------------------------")
    val dataMinCorePair: Array[Array[(String, Int)]] = Array(
      Array("/ours/SZT/all/SZT-2018Nov-mini-25", "/ours/SZT/all/SZT-2018Nov-mini-50", "/ours/SZT/all/SZT-2018Nov-mini-75", "/ours/SZT/all/SZT-2018Nov-mini-100"),
      Array("/ours/HZ/all/HZ-2019Jan-mini-25", "/ours/HZ/all/HZ-2019Jan-mini-50", "/ours/HZ/all/HZ-2019Jan-mini-75", "/ours/HZ/all/HZ-2019Jan-mini-100")
    ).map(_.zip(Range(48, 240, 16 * 3)))
    for (data <- dataMinCorePair) {
      println("——————————————————————————————————————————————————————————————————————")
      for (dataSR_core <- data) {
        val dataOri = dataSR_core._1
        val coresNum = dataSR_core._2
        val args = Array((coresNum * 2).toString, dataOri, "0.8", coresNum.toString, master)
        println("··································································")
        new join().main(args)
      }
      println("——————————————————————————————————————————————————————————————————————")
    }

  }
}