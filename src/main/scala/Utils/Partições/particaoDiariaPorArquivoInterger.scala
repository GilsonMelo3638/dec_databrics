package Utils.Particições

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object particaoDiariaPorArquivoInterger {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BPE Partitioning Final")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    try {

      val inputPath = "/datalake/bronze/sources/dbms/legado/dec/cte_diario_complemento"
      val outputPath = "/datalake/bronze/sources/dbms/legado/dec/cte_diario/"

      processSingleDirectory(spark, inputPath, outputPath)

      println("✅ Processamento concluído com sucesso!")

    } finally {
      // spark.stop()
    }
  }

  private def processSingleDirectory(
                                      spark: SparkSession,
                                      inputPath: String,
                                      outputPath: String): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(new Path(inputPath))) {

      try {

        val df = spark.read.parquet(inputPath)

        val partitionedDF =
          df.withColumn(
              "year",
              substring(col("DHPROC"), 7, 4).cast(IntegerType)
            )
            .withColumn(
              "month",
              substring(col("DHPROC"), 4, 2).cast(IntegerType)
            )
            .withColumn(
              "day",
              substring(col("DHPROC"), 1, 2).cast(IntegerType)
            )

        val numPartitions = 4

        partitionedDF
          .repartition(numPartitions)
          .write
          .partitionBy("year", "month", "day")
          .mode("append")
          .option("compression", "lz4")
          .parquet(outputPath)

        println(s"✔ Diretório $inputPath processado com sucesso")

      } catch {
        case e: Exception =>
          println(s"❌ Erro ao processar $inputPath: ${e.getMessage}")
      }

    } else {

      println(s"⚠️ Diretório $inputPath não encontrado")

    }
  }
}

// Execução
// particaoDiariaPorArquivoInterger.main(Array())