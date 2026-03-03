package RepartitionJob


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.math.ceil

object RepartitionXlmPequenosMediosProcessor {

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
      ("/datalake/prata/sources/dbms/dec/cte/CTe"),
      ("/datalake/prata/sources/dbms/dec/cte/CTeOS"),
      ("/datalake/prata/sources/dbms/dec/cte/GVTe"),
      ("/datalake/prata/sources/dbms/dec/bpe/BPe"),
      ("/datalake/prata/sources/dbms/dec/mdfe/MDFe"),
      ("/datalake/prata/sources/dbms/dec/nf3e/NF3e"),
      ("/datalake/prata/sources/dbms/dec/nfcom/NFCom")
    )

    configs.foreach { path =>
      println(s"Processando caminho: $path")
      processPartitions(path)
      println(s"Concluído processamento de: $path")
    }
  }
}

//RepartitionXlmPequenosMediosProcessor.main(Array())