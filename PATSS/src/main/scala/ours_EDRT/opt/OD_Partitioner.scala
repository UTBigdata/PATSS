package ours_EDRT.opt

import org.apache.spark.Partitioner

class OD_Partitioner(numPar: Int) extends Partitioner {
  assert(numPar > 0)

  override def numPartitions: Int = numPar

  override def getPartition(key: Any): Int = key match {
    case _ => key.asInstanceOf[Int] % numPar
  }
}
