package Utils.Particições

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object particaoDiariaPorArquivo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BPE Partitioning Final")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    try {
      val inputPath = "/datalake/bronze/sources/dbms/dec/nf3e/202307_202501"
      val outputPath = "/datalake/bronze/sources/dbms/legado/dec/nf3e_diario/"

      processSingleDirectory(spark, inputPath, outputPath)
      println("✅ Processamento concluído com sucesso!")
    } finally {
//      spark.stop()
    }
  }

  private def processSingleDirectory(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(new Path(inputPath))) {
      try {
        val df = spark.read.parquet(inputPath)

        val partitionedDF = df.withColumn("year", substring(col("DHPROC"), 7, 4))
          .withColumn("month", substring(col("DHPROC"), 4, 2))
          .withColumn("day", substring(col("DHPROC"), 1, 2))

        // Número fixo de partições baseado no tamanho esperado
        val numPartitions = 2 // Ajuste conforme necessário

        partitionedDF.repartition(numPartitions)
          .write
          .partitionBy("year", "month", "day")
          .mode("append")
          .option("compression", "lz4")
          .parquet(outputPath)

        println(s"✔ Diretório $inputPath processado com sucesso")
      } catch {
        case e: Exception => println(s"❌ Erro ao processar $inputPath: ${e.getMessage}")
      }
    } else {
      println(s"⚠️ Diretório $inputPath não encontrado")
    }
  }
}

// Execução
//particaoDiariaPorArquivo.main(Array())