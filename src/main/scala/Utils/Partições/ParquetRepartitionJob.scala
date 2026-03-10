package Utils.Particições

import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetRepartitionJob(spark: SparkSession) {

  def run(
           basePath: String,
           year: String,
           month: String,
           day: String,
           numPartitions: Int = 10,
           compression: String = "lz4"
         ): Unit = {

    val inputPath =
      s"$basePath/year=$year/month=$month/day=$day/"

    val outputPath =
      s"$basePath/year=$year/month=$month/day=${day}_2"

    spark.conf.set("spark.sql.parquet.compression.codec", compression)

    println(s"Lendo: $inputPath")

    val df: DataFrame = spark.read.parquet(inputPath)

    println(s"Repartitionando para $numPartitions arquivos")

    df.repartition(numPartitions)
      .write
      .mode("overwrite")
      .parquet(outputPath)

    println("Repartition concluído com sucesso.")
  }
}

//val spark = SparkSession.builder().appName("ParquetRepartition").getOrCreate()
//
//val job = new ParquetRepartitionJob(spark)
//
//job.run(
//  basePath = "/datalake/bronze/sources/dbms/legado/dec/nfe_diario",
//  year = "2024",
//  month = "08",
//  day = "05",
//  numPartitions = 10
//)

