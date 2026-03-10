package Utils.CorrecaoSchemaTransferirLegado

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.ceil

object NFCeRepartionGeralProcessor {

  val TARGET_MB = 256.0

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NFCeRepartionGeralProcessor")
      .getOrCreate()

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    val inputPath = "/datalake/bronze/sources/dbms/legado/dec/nfce_diario3/"
    val outputPath = "/datalake/bronze/sources/dbms/legado/dec/nfce_diario5/"

    try {

      val df = spark.read.parquet(inputPath)
        .withColumn("NSU", col("NSU").cast(StringType))
        .withColumn("CSTAT", col("CSTAT").cast(StringType))

      processByHDFS(spark, df, inputPath, outputPath)

    } finally {
      spark.stop()
    }
  }

  def processByHDFS(
                     spark: SparkSession,
                     df: DataFrame,
                     inputPath: String,
                     outputPath: String
                   ): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val years = fs.listStatus(new Path(inputPath))
      .filter(_.isDirectory)

    years.foreach { yearStatus =>

      val yearDir = yearStatus.getPath.getName // year=2021
      val yearValue = yearDir.split("=")(1).toInt

      val months = fs.listStatus(yearStatus.getPath)
        .filter(_.isDirectory)

      months.foreach { monthStatus =>

        val monthDir = monthStatus.getPath.getName // month=01 ou month=1
        val monthValue = monthDir.split("=")(1).toInt

        val days = fs.listStatus(monthStatus.getPath)
          .filter(_.isDirectory)

        days.foreach { dayStatus =>

          val dayDir = dayStatus.getPath.getName
          val dayValue = dayDir.split("=")(1).toInt

          val outputDayPath =
            new Path(s"$outputPath/$yearDir/$monthDir/$dayDir")

          if (fs.exists(outputDayPath)) {

            println(s"⏭ Já processado: $yearDir/$monthDir/$dayDir")

          } else {

            val sizeBytes = getFolderSize(fs, dayStatus.getPath)
            val sizeMB = sizeBytes / (1024.0 * 1024.0)

            val numPartitions =
              math.max(1, ceil(sizeMB / TARGET_MB).toInt)

            println(
              s"🚀 Processando $yearDir/$monthDir/$dayDir | ${sizeMB.formatted("%.2f")} MB | Arquivos: $numPartitions"
            )

            val dayDF = df.filter(
              col("year") === yearValue &&
                col("month") === monthValue &&
                col("day") === dayValue
            )

            dayDF
              .repartition(numPartitions)
              .write
              .mode("append")
              .option("compression", "lz4")
              .partitionBy("year", "month", "day")
              .parquet(outputPath)
          }
        }
      }
    }
  }

  def getFolderSize(fs: FileSystem, path: Path): Long = {
    val files = fs.listStatus(path)
    files.map { status =>
      if (status.isFile) status.getLen
      else getFolderSize(fs, status.getPath)
    }.sum
  }
}


//NFCeRepartionGeralProcessor.main(Array())