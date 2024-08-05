package ours_EDRT.tool

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, collect_list, concat_ws, explode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ours_LCPS.opt.{OD_Partitioner, heuristicPartition, neighborExpansion}

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class trajectoryInitial {

  def start(staTraOri01:RDD[Array[String]],spark:SparkSession,parNum:String,path:String,tim:Timer,optPar:Boolean): (RDD[(Int, String)], DataFrame, DataFrame) = {
    import spark.implicits._
    val args = Array(parNum,path)

    // lineStaSort: Map<line,stations>       lineStaSort.get("一号线").toArray.head.split(",").contains("")
    val lineStaSortSZT = mutable.Map[String, String]()
    lineStaSortSZT.put("四号线", "清湖站,龙华站,龙胜站,上塘站,红山站,深圳北站,白石龙站,民乐站,上梅林站,莲花北站,少年宫站,市民中心站,福民站,福田口岸站");
    lineStaSortSZT.put("九号线", "深湾站,深圳湾公园站,下沙站,香梅站,梅景站,下梅林站,梅村站,上梅林站,孖岭站,银湖站,泥岗站,红岭北站,园岭站,红岭站,红岭南站,鹿丹村站,人民南站,向西村站,文锦站");
    lineStaSortSZT.put("十一号线", "碧头站,松岗站,后亭站,沙井站,马安山站,塘尾站,桥头站,福永站,机场北站,机场站,碧海湾站,宝安站,前海湾站,南山站,后海站,红树湾南站");
    lineStaSortSZT.put("三号线", "益田站,石厦站,购物公园站,福田站,少年宫站,莲花村站,华新站,通新岭站,红岭站,老街站,晒布站,翠竹站,田贝站,水贝站,草埔站,布吉站,木棉湾站,大芬站,丹竹头站,六约站,塘坑站,横岗站,永湖站,荷坳站,大运站,爱联站,吉祥站,龙城广场站,南联站,双龙站");
    lineStaSortSZT.put("五号线", "黄贝岭站,怡景站,太安站,布心站,百鸽笼站,布吉站,长龙站,下水径站,上水径站,杨美站,坂田站,五和站,民治站,深圳北站,长岭陂站,塘朗站,大学城站,西丽站,留仙洞站,兴东站,洪浪北站,灵芝站,翻身站,宝华站,临海站");
    lineStaSortSZT.put("二号线", "新秀站,湖贝站,大剧院站,燕南站,华强北站,岗厦北站,市民中心站,莲花西站,景田站,香梅北站,香蜜站,侨香站,安托山站,深康站,侨城北站,红树湾站,科苑站,后海站,登良站,海月站,湾厦站,东角头站,水湾站,海上世界站,蛇口港站,赤湾站");
    lineStaSortSZT.put("一号线", "机场东站,后瑞站,固戍站,西乡站,坪洲站,宝体站,宝安中心站,新安站,前海湾站,鲤鱼门站,大新站,桃园站,深大站,高新园站,白石洲站,世界之窗站,华侨城站,侨城东站,竹子林站,车公庙站,香蜜湖站,购物公园站,会展中心站,岗厦站,华强路站,科学馆站,大剧院站,国贸站,罗湖站");
    lineStaSortSZT.put("七号线", "西丽湖站,茶光站,珠光站,龙井站,桃源村站,深云站,农林站,上沙站,沙尾站,皇岗村站,福民站,皇岗口岸站,赤尾站,华强南站,华强北站,黄木岗站,八卦岭站,笋岗站,洪湖站")

    val lineStaSortHZ = mutable.Map[String, String]()
    lineStaSortHZ.put("A","67,68,69,70,71,72,73,74,75,76,77,78,79,80")
    lineStaSortHZ.put("B","0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33")
    lineStaSortHZ.put("C","34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66")

    var lineStaSort = mutable.Map[String, String]()
    path match {
      case _ if path.contains("SZT") => lineStaSort = lineStaSortSZT
      case _ if path.contains("HZ") => lineStaSort = lineStaSortHZ
    }
    val flag = if (path.contains("HZ-2019Jan-mini-25")) true else false

    tim.restart()
    val staTraOri13 = staTraOri01.map { tra: Array[String] =>
        // val tra = staTraOri0.take(10)(8)
        // 20181122 18:00:00.百鸽笼站.地铁五号线.I,20181122 18:10:00.长龙站.地铁五号线.I,20181122 18:20:00.百鸽笼站.地铁五号线.O,2018119:40:00.六约站.地铁三号线.O
        val traArr: Array[String] = tra(1).split(",")
        val time4: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val traSort = traArr.sorted
        val sta_OD_tra = ArrayBuffer[String]() //ArrayBuffer(新安站(一号线<>洪浪北站(五号线, 洪浪北站(五号线<>翻身站(五号线, 新安站(一号线<>洪浪北站(五号线, 新安站(一号线<>洪浪北站(五号线, 新安站(一号线 <> 洪浪北站(五号线, 新安站(一号线 <> 洪浪北站(五号线)
        val sta_OD_tra_time = ArrayBuffer[String]() //ArrayBuffer(20181101 06:57:01.新安站.一号线.I, 20181101 07:12:34.洪浪北站.五号线.O, 20181101 08:16:47.洪浪北站.五号线.I, 20181101 08:22:01.翻身站.五号线.O, 20181102 07:08:06.新安站.一号线.I, 20181102 07:20:54.洪浪北站.五号线.O)
        var x = 0
        while (x < traSort.length - 1) {
          if (traArr(x).contains("I") && traArr(x + 1).contains("O")) {
            if (
              time4.parse(traArr(x + 1).split("[.]")(0)).getDay ==
                time4.parse(traArr(x).split("[.]")(0)).getDay &&
                (time4.parse(traArr(x + 1).split("[.]")(0)).getTime -
                  time4.parse(traArr(x).split("[.]")(0)).getTime) / 1000 / 60 < 18 * 60
            ) {
              sta_OD_tra += traArr.slice(x, x + 2).map(x => x.split("[.]")(1) + "(" + x.split("[.]")(2)).mkString("<>") //左闭右开  洪浪北站(五号线<>宝安中心站(一号线, 洪浪北站(五号线<>宝安中心站(一号线
              sta_OD_tra_time += traArr(x);
              sta_OD_tra_time += traArr(x + 1)
              x = x + 2
            }
            else x = x + 2
          }
          else x = x + 1
        }

        var sta_OD_F = "no_sta_OD_F"
        var sta_OD_S = "no_sta_OD_S"
        var sta_OD_F_S = "no_sta_OD_F_S"
        var line_OD_F: String = "no_line_OD_F"
        var line_OD_S: String = "no_line_OD_S"
        var line_OD_F_S: String = "no_line_OD_F_S"
        var CH = 0
        var CS = 0
        if (sta_OD_tra.nonEmpty) { //莲花北站(四号线<>民治站(五号线<>17-民治站(五号线<>莲花北站(四号线<>17    17是CH和CS
          val sta_line_OD_F_S = sta_OD_tra.map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).
            toArray.sortWith(_._2 > _._2).take(2).map(x => x._1 + "<>" + x._2).mkString("-")
          //求sta_OD_F_S    莲花北站<>民治站-民治站<>莲花北站
          val sta_OD_F_S_medi = sta_line_OD_F_S.split("-").map(x => x.split("<>").dropRight(1).map(_.split("[(]")(0)).
            mkString("<>")).mkString("-")
          if (sta_OD_F_S_medi.split("-").length > 1) {
            sta_OD_F = sta_OD_F_S_medi.split("-")(0)
            sta_OD_S = sta_OD_F_S_medi.split("-")(1)
            sta_OD_F_S = sta_OD_F_S_medi
          } else {
            sta_OD_F = sta_OD_F_S_medi.split("-")(0);
            sta_OD_F_S = sta_OD_F + "-" + sta_OD_S
          }
          //求line_sortOD_F-S   四号线<>五号线=17-五号线<>四号线=17
          val line_OD_F_S_medi = sta_line_OD_F_S.split("-").map(x => x.split("<>").dropRight(1).map(_.split("[(]")(1)).
            mkString("<>")).mkString("-")
          if (line_OD_F_S_medi.split("-").length > 1) {
            line_OD_F = line_OD_F_S_medi.split("-")(0)
            line_OD_S = line_OD_F_S_medi.split("-")(1)
            line_OD_F_S = line_OD_F_S_medi
            CH = sta_line_OD_F_S.split("-").map(x => x.split("<>").last).head.toInt
            CS = sta_line_OD_F_S.split("-").map(x => x.split("<>").last).last.toInt
          }
          else {
            line_OD_F = line_OD_F_S_medi.split("-")(0)
            line_OD_F_S = line_OD_F + "-" + line_OD_S
            CH = sta_line_OD_F_S.split("-").map(x => x.split("<>").last).head.toInt
          }
          //求CH和CS   要注意：sta_line_OD_F_S: String = 红岭北站(九号线<>怡景站(五号线<>12  这样没有SOD的情况
          (tra(0), sta_OD_F, sta_OD_S, sta_OD_F_S, line_OD_F, line_OD_S, line_OD_F_S, CH, CS, sta_OD_tra.length * 2, sta_OD_tra.length, sta_OD_tra_time) //原始的poitnTra:traSort.mkString(",");sta_OD_tra_time是原始的pointTra去掉了异常值，可以组成OD，形式依然为point，且没有时间规整
        }
        else (tra(0), sta_OD_F, sta_OD_S, sta_OD_F_S, line_OD_F, line_OD_S, line_OD_F_S, CH, CS, sta_OD_tra.length * 2, sta_OD_tra.length, sta_OD_tra_time) //traSort.mkString(","),
      }.toDF("card", "sta_OD_F", "sta_OD_S", "sta_OD_F_S", "line_OD_F", "line_OD_S", "line_OD_F_S", "CH", "CS", "pointTraLen", "ODTraLen", "pointTra").
      filter($"sta_OD_F" =!= "no_sta_OD_F").filter($"ODTraLen" > "4").toDF("card", "sta_OD_F", "sta_OD_S", "sta_OD_F_S", "line_OD_F", "line_OD_S", "line_OD_F_S", "CH", "CS", "pointTraLen", "ODTraLen", "pointTra").
      select($"line_OD_F_S", $"line_OD_F", $"line_OD_S", $"sta_OD_F_S", $"sta_OD_F", $"sta_OD_S", $"CH", $"CS", $"pointTraLen", $"ODTraLen",
        concat_ws("#", $"card", $"pointTra").
          cast(StringType).
          as("card#pointTra")).
      toDF(
        "line_OD_F_S", "line_OD_F", "line_OD_S",
        "sta_OD_F_S", "sta_OD_F", "sta_OD_S", "CH", "CS", "pointTraLen", "ODTraLen",
        "card#pointTra")

    val partitionNum = args(0).toDouble
    staTraOri13.persist()
    val cardCount = staTraOri13.select("card#pointTra").count().toDouble
    val eachParCount = (cardCount / partitionNum)//.ceil.toInt
    val read = tim.elapsed()
    println(s"trajCount is $cardCount")
    println(s"read data ${args(1)} is $read(s)")

    tim.restart()
    val b = staTraOri13.
      groupBy(
        $"line_OD_F_S"
      ).agg(collect_list("sta_OD_F").as("sta_OD_F_ALL")). //sta_OD_F_ALL 每一个sta_OD_F都是一个乘客
      map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[mutable.WrappedArray[String]].mkString(","))).rdd

    //c:需矩阵分块的索引（以staODH的卡号总数为据） (f,e.toMap,e.toMap.values.sum) //f:一号线<>一号线-十一号线<>九号线  e.toMap.values.sum：索引里包含的卡号数量
    val c = b.map { x: (String, String) =>
      val f = x._1
      val e = x._2.split(",").map(x => (x, 1)).groupBy(_._1).map(t => (t._1, t._2.length)).toArray //.sortWith(_._2 > _._2).take(1) //不用排序
      (f, e.toMap, e.toMap.values.sum) // Map(桃园站<>西乡站 -> 3, 桃园站<>大新站 -> 2, 固戍站<>后瑞站 -> 1, 固戍站<>固戍站 -> 2, 侨城东站<>宝体站 -> 1, 桃园站<>世界之窗站 -> 6, 侨城东站<>西乡站 -> 2, 会展中心站<>华强路站 -> 2, 侨城东站<>会展中心站 -> 1, 机场东站<>高新园站 -> 1, 桃园站<>华侨城站 -> 1, 白石洲站<>宝体站 -> 3,
    }.filter(_._3 > eachParCount * 1.1) //eachParCount

    //生成LINE H OD 的 segs HO  和 segs HD
    val LineFSOD_LFsegOD = c.map { mapLOD =>
      val lineODH = mapLOD._1.split("-")(0);
      val LHO = lineODH.split("<>")(0);
      val LHD = lineODH.split("<>")(1)
      val staODH: Map[String, Int] = mapLOD._2 //Map(侨城东站<>龙胜站 -> 12, 桃园站<>民乐站 -> 39, 深大站<>清湖站 -> 56, 高新园站<>少年宫站 -> 16, 白石洲站<>市民中心站 -> 34, 大剧院站<>清湖站 -> 76
      val LHOstas = lineStaSort.get(LHO).toArray.head.split(",");
      val LHDstas = lineStaSort.get(LHD).toArray.head.split(",")
      val dimStaHOD: Array[Array[Int]] = Array.ofDim[Int](LHOstas.length, LHDstas.length)
      for (staODCount <- staODH) { //填充由HOSTA和HDSTA构成的矩阵，值为staOD对应的轨迹数量大小
        val staOD = staODCount._1;
        val staO = staOD.split("<>")(0);
        val staD = staOD.split("<>")(1)
        val m = lineStaSort.get(LHO).toArray.head.split(",").indexOf(staO) //      val m = lineStaSort.get("四号线").toArray.head.split(",").indexOf("清湖站")
        val n = lineStaSort.get(LHD).toArray.head.split(",").indexOf(staD)
        dimStaHOD(m)(n) = staODCount._2
      } //填充dimStaHOD
      val SOD = new neighborExpansion().splitMat(dimStaHOD, eachParCount, LHOstas, LHDstas)
      val SOs = SOD._1.mkString(",")
      val SDs = SOD._2.mkString(",")
      val SODs = Array(SOs, SDs).mkString(";")
      (mapLOD._1, SODs)
      //      (mapLOD._1,SODs,SOD._3.map(_.mkString(",")).mkString(";"))
    }.toDF("LineFSOD", "LFsegOD") //.toDF("LineFSOD","LFsegOD","splitMatrix")

    //lineFSOD_LFsegOD变成map后广播
    val bc: Array[Row] = LineFSOD_LFsegOD.collect()
    val MMap = mutable.Map[String, String]()
    // Map(二号线<>七号线-七号线<>二号线 ->
    // 新秀站-湖贝站-大剧院站-燕南站,华强北站,岗厦北站-市民中心站-莲花西站-景田站-香梅北站-香蜜站-侨香站-安托山站-深康站-侨城北站-红树湾站-科苑站-后海站-登良站-海月站-湾厦站-东角头站-水湾站-海上世界站-蛇口港站-赤湾站;
    // 西丽湖站-茶光站-珠光站-龙井站-桃源村站-深云站-农林站-上沙站-沙尾站-皇岗村站-福民站-皇岗口岸站,赤尾站,华强南站-华强北站-黄木岗站-八卦岭站-笋岗站-洪湖站)
    bc.foreach(row => MMap.put(row(0).asInstanceOf[String], row(1).asInstanceOf[String]))
    val bcLineSegMap = spark.sparkContext.broadcast(MMap)

    //生成所有1422372张卡的全局索引:
    val traGlobal = staTraOri13.map { x => // |line_OD_F_S |line_OD_F|line_OD_S|sta_OD_F_S|sta_OD_F |sta_OD_S|CH |CS |pointTraLen|ODTraLen|card#pointTra|
      val needSplitLineSig = bcLineSegMap.value.keys.toArray
      val line_OD_F_S = x(0).asInstanceOf[String]
      if (needSplitLineSig.contains(line_OD_F_S)) {
        val lineHOD = x(1).asInstanceOf[String].split("<>")
        val segs: Array[String] = bcLineSegMap.value.get(line_OD_F_S).toArray.head.split(";")
        val lineSegsO: String = segs(0)
        val lineSegsD: String = segs(1)
        val sta_OD_F = x(4).asInstanceOf[String].split("<>") //梅景站<>通新岭站
        val staFO = sta_OD_F.head;
        val staFD = sta_OD_F.last
        val segSigO: String = lineSegsO.split(",").filter(x => x.contains(staFO)).head
        val segSigD: String = lineSegsD.split(",").filter(x => x.contains(staFD)).head
        //        val segSigOidx = lineSegO.split(",").indexOf(segSigO)
        //        val segSigDidx = lineSegD.split(",").indexOf(segSigD)
        val line_OD_S = x(2).asInstanceOf[String]
        val seg_OD_F_S: String = segSigO + "<>" + segSigD + "[+]" + line_OD_S
        (line_OD_F_S, x(1).asInstanceOf[String], x(2).asInstanceOf[String],
          seg_OD_F_S, segSigO + "<>" + segSigD,
          x(3).asInstanceOf[String], x(4).asInstanceOf[String], x(5).asInstanceOf[String],
          x(6).asInstanceOf[Int], x(7).asInstanceOf[Int],
          x(8).asInstanceOf[Int], x(9).asInstanceOf[Int],
          x(10).asInstanceOf[String]
        )
      }
      else (x(0).asInstanceOf[String], x(1).asInstanceOf[String], x(2).asInstanceOf[String],
        x(0).asInstanceOf[String].split("-").mkString("[+]"), x(1).asInstanceOf[String],
        x(3).asInstanceOf[String], x(4).asInstanceOf[String], x(5).asInstanceOf[String],
        x(6).asInstanceOf[Int], x(7).asInstanceOf[Int],
        x(8).asInstanceOf[Int], x(9).asInstanceOf[Int],
        x(10).asInstanceOf[String]
      )
    }.toDF("line_OD_F_S", "line_OD_F", "line_OD_S",
      "seg_OD_F_S", "seg_OD_F",
      "sta_OD_F_S", "sta_OD_F", "sta_OD_S",
      "CH", "CS",
      "pointTraLen", "ODTraLen",
      "card#pointTra")

    //    staTraOri13.unpersist()
    traGlobal.persist()

    //大数据量用启发式优化分区策略
    if(optPar){//!path.contains("mini")
      val waitCoIndex: mutable.ArrayBuffer[(String, Int)] = traGlobal.
        select($"card#pointTra", concat_ws("##",
          $"line_OD_F_S", $"seg_OD_F_S"). //加上line_OD_F_S是因为也要存lineODFS对应的分区号，为了segODFS找不全，扩大到line范围寻找的情况。合并后的大小依然同seg_OD_F_S
          cast(StringType).as("line_OD_F_S##seg_OD_F_S")).groupBy("line_OD_F_S##seg_OD_F_S").count().
        collect().map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[Long].toInt)).toBuffer.asInstanceOf[ArrayBuffer[(String, Int)]]

      //    println(waitCoIndex.map(_._2).mkString(","))
      //把line_OD_F_S##seg_OD_F_S分到指定数量的分区里，保证每个分区中所有的sig: line_OD_F_S##seg_OD_F_S对应的轨迹数量之和在一个合理范围内，保证数据分布均匀。
      val coIndex: Array[String] = new heuristicPartition().GA(waitCoIndex, eachParCount, partitionNum.toInt, hzmini25 = flag) //heuristicPartition().GA输入为 line_OD_F_S##seg_OD_F_S count
      //coIndex：signature分组情况。每个分组对应一个分区。

      //通过signature定位该cardRDD分区号  //小dataframe 广播这个！    parTH |sig_Line |sig_Seg
      val indexParTH: DataFrame = spark.sparkContext.parallelize(coIndex.zipWithIndex).
        map(x => (x._1.split("&"), x._2)).toDF("sigArr", "parTH").
        withColumn(
          "sig", explode($"sigArr") //一行变多行
        ).select("sig", "parTH").
        map(x => (x(0).asInstanceOf[String].split("##"), x(1).asInstanceOf[Int])).toDF("sigLS", "parTH").select(
          $"parTH" +: (0 until 2).map(i => $"sigLS"(i).alias(s"sig$i")): _*
        ).toDF("parTH", "sig_Line", "sig_Seg")

      //广播连接|card|index|（lineTra1）和|index|pTH|（indexParTH）,得到每个卡号及其各种轨迹和属性应在哪一个分区
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) //取消要求广播的表的大小不超过10M的限制//println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))//spark.conf.set( "spark.sql.crossJoin.enabled",value = true) //下面这句居然是隐式的笛卡尔积？？离谱。org.apache.spark.sql.AnalysisException: Detected implicit cartesian product for INNER join between logical plans
      //把每张卡按照自定义分区放到相应的分区里 RDD(card#Attr-cTH_PTH-In-dex）

      val traGlobal1: RDD[(Int, String)] = traGlobal.join(broadcast(indexParTH), $"seg_OD_F_S" === $"sig_Seg").
        drop($"sig_Seg").select($"parTH", concat_ws("###",
          $"card#pointTra", $"line_OD_F_S", $"seg_OD_F_S", $"sta_OD_F_S", $"CH", $"CS", $"pointTraLen", $"ODTraLen").
          cast(StringType).as("card###Attr")). //RDD Pair自定义分区   cardAttrIndex.getNumPartitions=576 ✔
        map(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[String])).rdd.partitionBy(new OD_Partitioner(args(0).toInt))

      val cardSigAttrDF = traGlobal1.map(_._2.split("###")).toDF("d").
        select((0 until 8).map(i => $"d"(i).alias(s"d$i")): _*).
        toDF("card#pointTra", "line_OD_F_S", "seg_OD_F_S", "sta_OD_F_S", "CH", "CS",
          "pointTraLen", "ODTraLen")
      //    traGlobal.unpersist()
      (traGlobal1, indexParTH, cardSigAttrDF)
    }
    else{//mini数据用"默认分区策略"
      val traGlobal1: RDD[(Int, String)] = spark.sparkContext.parallelize(Seq((0,"")))
      val indexParTH: DataFrame = spark.createDataFrame(Seq((0,"")))
      val cardSigAttrDF = traGlobal.select($"card#pointTra", $"line_OD_F_S", $"seg_OD_F_S", $"sta_OD_F_S", $"CH", $"CS",
        $"pointTraLen", $"ODTraLen")
      (traGlobal1, indexParTH, cardSigAttrDF)
    }

  }
}
