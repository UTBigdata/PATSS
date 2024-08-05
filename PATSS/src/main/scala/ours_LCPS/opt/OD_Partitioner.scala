package ours_LCPS.opt

import org.apache.spark.Partitioner

class OD_Partitioner(numPar: Int) extends Partitioner {
  assert(numPar > 0) // 返回分区数, 必须要大于0.

  override def numPartitions: Int = numPar

  //返回指定键的分区编号(0到numPartitions-1)
  override def getPartition(key: Any): Int = key match {
    //      case null => 0
    case _ => key.asInstanceOf[Int] % numPar
  }
}
