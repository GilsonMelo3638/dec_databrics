package Utils.Partições

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.math.ceil

class ParquetRepartitionJob(spark: SparkSession) {

  val TARGET_MB = 256.0

  def run(
           basePath: String,
           year: String,
           month: String,
           day: String,
           compression: String = "lz4"
         ): Unit = {

    val inputPath =
      s"$basePath/year=$year/month=$month/day=$day/"

    val outputPath =
      s"$basePath/year=$year/month=$month/day=${day}_2"

    spark.conf.set("spark.sql.parquet.compression.codec", compression)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    println(s"Lendo: $inputPath")

    val df: DataFrame = spark.read.parquet(inputPath)

    // calcula tamanho total da pasta
    val sizeBytes = getFolderSize(fs, new Path(inputPath))
    val sizeMB = sizeBytes / (1024.0 * 1024.0)

    val numPartitions =
      math.max(1, ceil(sizeMB / TARGET_MB).toInt)

    println(
      f"Tamanho da partição: $sizeMB%.2f MB | Arquivos alvo: $numPartitions"
    )

    df.repartition(numPartitions)
      .write
      .mode("overwrite")
      .parquet(outputPath)

    println("Repartition concluído com sucesso.")
  }

  def getFolderSize(fs: FileSystem, path: Path): Long = {
    val files = fs.listStatus(path)

    files.map { status =>
      if (status.isFile) status.getLen
      else getFolderSize(fs, status.getPath)
    }.sum
  }
}

//val job = new ParquetRepartitionJob(spark)
//
//job.run(
//  basePath = "/datalake/bronze/sources/dbms/legado/dec/nfce_diario2",
//  year = "2024",
//  month = "8",
//  day = "9"
//)
