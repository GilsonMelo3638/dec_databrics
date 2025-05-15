package Utils.Particoes

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object particaoDiariaPorAno {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NFE Partitioning Final")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    try {
      val basePath = "/datalake/bronze/sources/dbms/dec/"
      val inputBase = s"${basePath}nfe_evento/"
      val outputPath = "/datalake/bronze/sources/dbms/legado/dec/nfe_evento_diario/"

      // Parâmetros flexíveis para o período de processamento
      val startYear = 2025
      val endYear = 2025

      processPeriod(spark, inputBase, outputPath, startYear, endYear)
      println("✅ Processamento concluído com sucesso!")
    } finally {
      spark.stop()
    }
  }

  private def processPeriod(
                             spark: SparkSession,
                             inputBase: String,
                             outputPath: String,
                             startYear: Int,
                             endYear: Int
                           ): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    (startYear to endYear).foreach { year =>
      val yearStr = year.toString
      val inputPath = s"$inputBase$yearStr/"

      if (fs.exists(new Path(inputPath))) {
        try {
          processSingleYear(spark, inputPath, outputPath, yearStr)
          println(s"✔ $yearStr processado com sucesso")
        } catch {
          case e: Exception => println(s"❌ Erro ao processar $yearStr: ${e.getMessage}")
        }
      } else {
        println(s"⚠️ Pasta $yearStr não encontrada")
      }
    }
  }

  private def processSingleYear(spark: SparkSession, inputPath: String, outputPath: String, yearStr: String): Unit = {
    val df = spark.read.parquet(inputPath)

    val partitionedDF = df.withColumn("year", substring(col("DHPROC"), 7, 4))
      .withColumn("month", substring(col("DHPROC"), 4, 2))
      .withColumn("day", substring(col("DHPROC"), 1, 2))

    // Número fixo de partições baseado no tamanho esperado
    val numPartitions = 4 // Ajuste conforme necessário

    partitionedDF.repartition(numPartitions)
      .write
      .partitionBy("year", "month", "day")
      .mode("append")
      .option("compression", "lz4")
      .parquet(outputPath)
  }
}

// Execução
//particaoDiariaPorAno.main(Array())