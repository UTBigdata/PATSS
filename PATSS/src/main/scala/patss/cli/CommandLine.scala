package patss.cli

object LCPSSearch {
  def main(args: Array[String]): Unit = {
    require(args.length == 6, "Usage: LCPSSearch <partitions> <trajectory-input> <tau> <query-parquet> <total-executor-cores> <spark-master>")
    new patss.lcps.search().main(args)
  }
}

object EDRTSearch {
  def main(args: Array[String]): Unit = {
    require(args.length == 6, "Usage: EDRTSearch <partitions> <trajectory-input> <epsilon> <query-parquet> <total-executor-cores> <spark-master>")
    new patss.edrt.search().main(args)
  }
}

object LCPSJoin {
  def main(args: Array[String]): Unit = {
    require(args.length == 5, "Usage: LCPSJoin <partitions> <trajectory-input> <tau> <total-executor-cores> <spark-master>")
    new patss.lcps.join().main(args)
  }
}

object EDRTJoin {
  def main(args: Array[String]): Unit = {
    require(args.length == 5, "Usage: EDRTJoin <partitions> <trajectory-input> <epsilon> <total-executor-cores> <spark-master>")
    new patss.edrt.join().main(args)
  }
}
