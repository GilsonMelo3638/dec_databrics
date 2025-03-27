package Utils.Particições

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.{Try, Success, Failure}

object NfcePartitioner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NFCE Partitioning")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    try {
      val basePath = "/datalake/bronze/sources/dbms/dec/"
      val inputBase = s"${basePath}nfce/"
      val outputPath = s"${basePath}nfce_part/"

      processAllMonths(spark, inputBase, outputPath)
      println("✅ Processamento concluído com sucesso!")
    } finally {
      spark.stop()
    }
  }

  private def processAllMonths(spark: SparkSession, inputBase: String, outputPath: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Gera todos os meses do período
    val months = for {
      year <- 2019 to 2025
      month <- 1 to 12
      monthStr = f"$year$month%02d"
      if monthStr >= "201901" && monthStr <= "202502"
    } yield monthStr

    months.foreach { month =>
      val inputPath = s"$inputBase$month/"
      if (fs.exists(new Path(inputPath))) {
        Try(processSingleMonth(spark, inputPath, outputPath)) match {
          case Success(_) => println(s"✔ $month processado com sucesso")
          case Failure(e) => println(s"❌ Erro ao processar $month: " + e.getMessage)
        }
      } else {
        println(s"⚠️ Pasta $month não encontrada")
      }
    }
  }

  private def processSingleMonth(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val df = spark.read.parquet(inputPath)

    if (!df.columns.contains("DHPROC")) {
      throw new Exception(s"Coluna DHPROC não encontrada em $inputPath")
    }

    val partitionedDF = df
      .withColumn("year", substring(col("DHPROC"), 7, 4))
      .withColumn("month", substring(col("DHPROC"), 4, 2))
      .withColumn("day", substring(col("DHPROC"), 1, 2))
      .withColumn("hour", substring(col("DHPROC"), 12, 2))

    partitionedDF.write
      .partitionBy("year", "month", "day", "hour")
      .mode("append")
      .option("compression", "lz4")
      .parquet(outputPath)
  }
}

// Execução
NfcePartitioner.main(Array())