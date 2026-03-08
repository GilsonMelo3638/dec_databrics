package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CTeAppend {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CTeProcessor")
      .getOrCreate()

    // 🔴 ESSA LINHA RESOLVE O SEU ERRO
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    val inputPath = "/datalake/bronze/sources/dbms/legado/dec//cte_diario_complemento/"
    val outputPath = "/datalake/bronze/sources/dbms/legado/dec/cte_diario2/"

    val numPartitions = 10

    try {

      val df = spark.read.parquet(inputPath)

      val convertedDF = df
        .withColumn("NSUSVD", col("NSUSVD").cast(StringType))
        .withColumn("NSUAUT", col("NSUAUT").cast(StringType))
        .withColumn("CSTAT", col("CSTAT").cast(StringType))
        .withColumnRenamed("day", "original_day")

      processAndPartitionData(convertedDF, outputPath, numPartitions)

    } finally {
      spark.stop()
    }
  }

  def processAndPartitionData(df: DataFrame, outputPath: String, numPartitions: Int): Unit = {

    val finalDF = df
      .withColumnRenamed("original_day", "day")

    finalDF
      .repartition(numPartitions)
      .write
      .mode("append")
      .option("compression", "lz4")
      .partitionBy("year", "month", "day")
      .parquet(outputPath)
  }
}
//CTeAppend.main(Array())