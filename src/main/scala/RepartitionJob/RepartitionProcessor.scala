//scp "C:\dec\target\DecInfNFePrata-0.0.1-SNAPSHOT.jar"  gamelo@10.69.22.71:src/main/scala/DecInfNFePrata-0.0.1-SNAPSHOT.jar
//hdfs dfs -put -f /export/home/gamelo/src/main/scala/DecInfNFePrata-0.0.1-SNAPSHOT.jar /app/dec
//hdfs dfs -ls /app/dec
// hdfs dfs -rm -skipTrash /app/dec/DecInfNFePrata-0.0.1-SNAPSHOT.jar
// spark-submit \
//  --class DECJob.InfNFeProcessor \
//  --master yarn \
//  --deploy-mode cluster \
//  --num-executors 20 \
//  --executor-memory 4G \
//  --executor-cores 2 \
//  --conf "spark.sql.parquet.writeLegacyFormat=true" \
//  --conf "spark.sql.debug.maxToStringFields=100" \
//  --conf "spark.executor.memoryOverhead=1024" \
//  --conf "spark.network.timeout=800s" \
//  --conf "spark.yarn.executor.memoryOverhead=4096" \
//  --conf "spark.shuffle.service.enabled=true" \
//  --conf "spark.dynamicAllocation.enabled=true" \
//  --conf "spark.dynamicAllocation.minExecutors=10" \
//  --conf "spark.dynamicAllocation.maxExecutors=40" \
//  --packages com.databricks:spark-xml_2.12:0.13.0 \
//  hdfs://sepladbigdata/app/dec/DecInfNFePrata-0.0.1-SNAPSHOT.jar
package RepartitionJob


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.math.ceil

object RepartitionProcessor {

  val spark = SparkSession.builder().getOrCreate()
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)

  val TARGET_MB = 256.0
  val TOLERANCE_PERCENT = 0.30   // 10% de tolerância
  val TOLERANCE_ABSOLUTE = 1     // diferença absoluta de 1 é ignorada

  def main(args: Array[String]): Unit = {

    def getDirectorySize(path: Path): Long = {
      fs.listStatus(path).map { status =>
        if (status.isFile) status.getLen
        else getDirectorySize(status.getPath)
      }.sum
    }

    def processPartitions(basePath: String): Unit = {

      val partitions = fs.listStatus(new Path(basePath))
        .filter(_.isDirectory)
        .map(_.getPath.toString)

      partitions.foreach { partitionPath =>
        try {

          val path = new Path(partitionPath)

          val parquetFiles = fs.listStatus(path)
            .filter(f => f.isFile && f.getPath.getName.endsWith(".parquet"))

          val fileCount = parquetFiles.length

          val sizeBytes = getDirectorySize(path)
          val sizeMB = sizeBytes / (1024.0 * 1024.0)

          val idealPartitions =
            math.max(1, ceil(sizeMB / TARGET_MB).toInt)

          val diff = math.abs(fileCount - idealPartitions)
          val percentDiff =
            if (idealPartitions == 0) 0.0
            else diff.toDouble / idealPartitions.toDouble

          println(s"\nPartição: $partitionPath")
          println(f"Tamanho: $sizeMB%.2f MB")
          println(s"Arquivos atuais: $fileCount")
          println(s"Arquivos ideais: $idealPartitions")
          println(f"Diferença percentual: ${percentDiff * 100}%.2f%%")

          // 🔥 Nova regra inteligente
          val shouldRepartition =
            diff > TOLERANCE_ABSOLUTE &&
              percentDiff > TOLERANCE_PERCENT

          if (shouldRepartition) {

            println("Reorganizando partição...")

            val df = spark.read.parquet(partitionPath)
            val tempPath = s"${partitionPath}_temp"

            val repartitionedDF =
              if (idealPartitions > fileCount)
                df.repartition(idealPartitions)
              else
                df.coalesce(idealPartitions)

            repartitionedDF
              .write
              .option("compression", "lz4")
              .mode("overwrite")
              .parquet(tempPath)

            fs.delete(path, true)
            fs.rename(new Path(tempPath), path)

            println(s"Reorganização concluída para: $partitionPath")

          } else {
            println("Diferença irrelevante. Ignorando reorganização.")
          }

        } catch {
          case e: Exception =>
            System.err.println(s"Erro ao processar $partitionPath: ${e.getMessage}")
        }
      }
    }

    val configs = Seq(
      ("/datalake/prata/sources/dbms/dec/nfe/infNFe"),
      ("/datalake/prata/sources/dbms/dec/nfe/det"),
      ("/datalake/prata/sources/dbms/dec/nfce/infNFCe"),
      ("/datalake/prata/sources/dbms/dec/nfce/det")
    )

    configs.foreach { path =>
      println(s"Processando caminho: $path")
      processPartitions(path)
      println(s"Concluído processamento de: $path")
    }
  }
}


//RepartitionProcessor.main(Array())