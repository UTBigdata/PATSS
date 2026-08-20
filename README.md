# PATSS

PATSS is a Scala/Spark framework for similarity search and join over metro passenger trajectories.

## Source layout

- `PATSS/src/main/scala/patss/lcps`: LCPS search and join.
- `PATSS/src/main/scala/patss/edrt`: EDRT search and join.
- `PATSS/src/main/scala/patss/cli`: command-line entry points.
- `PATSS/src/main/scala/prepare`: Shenzhen and Hangzhou data preprocessing.
- `PATSS/src/main/scala/LCPS` and `PATSS/src/main/scala/EDRT`: experimental batch drivers.

## Requirements

- JDK 8
- Scala 2.11.12
- Apache Spark 2.4.0
- Hadoop 2.6.0
- sbt 1.2.7

Spark and Hadoop must be configured on the target cluster.

## Build

```bash
cd PATSS
sbt clean package
```

The JAR is generated under `PATSS/target/scala-2.11/`.

## Input

Search and join read Spark text input with one passenger trajectory per line:

```text
<passenger-id>\t<timestamp>.<station>.<line>.<I|O>,<timestamp>.<station>.<line>.<I|O>,...
```

Timestamps use `yyyy-MM-dd HH:mm:ss`. Each trip consists of a valid entry record and its corresponding exit record. The input path must contain `SZT` or `HZ` so that PATSS can select the Shenzhen or Hangzhou metro topology.

Search also requires an existing Parquet file whose first column contains the query representation expected by PATSS.

## Preprocessing

```bash
spark-submit --class prepare.HZ.traPreSample --master <spark-master> <jar> \
  <hangzhou-csv-path> <output-root> <spark-master>

spark-submit --class prepare.SZT.traPreSample --master <spark-master> <jar> \
  <shenzhen-csv-path-or-glob> <output-root> <spark-master>
```

The required CSV columns are defined in the corresponding source files.

## Similarity search

LCPS uses similarity threshold `tau`; EDRT uses edit-distance threshold `epsilon`.

```bash
spark-submit --class patss.cli.LCPSSearch --master <spark-master> <jar> \
  <partitions> <trajectory-input> <tau> <query-parquet> <total-executor-cores> <spark-master>

spark-submit --class patss.cli.EDRTSearch --master <spark-master> <jar> \
  <partitions> <trajectory-input> <epsilon> <query-parquet> <total-executor-cores> <spark-master>
```

## Similarity join

```bash
spark-submit --class patss.cli.LCPSJoin --master <spark-master> <jar> \
  <partitions> <trajectory-input> <tau> <total-executor-cores> <spark-master>

spark-submit --class patss.cli.EDRTJoin --master <spark-master> <jar> \
  <partitions> <trajectory-input> <epsilon> <total-executor-cores> <spark-master>
```

The batch drivers retain the dataset paths and parameter grids used in the experiments. Update those paths for the target file system.

## Data availability

- **Hangzhou Metro data:** available from the [Tianchi repository](https://tianchi.aliyun.com/dataset/128247).
- **Shenzhen Metro data:** not publicly available because of the confidentiality agreement under which the data were obtained, but available from the corresponding author on reasonable request.

No raw data or passenger identifiers are included in this repository.

## Citation

Please cite the associated PATSS manuscript or this repository. Publication details will be added after publication.
