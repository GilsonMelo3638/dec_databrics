package Utils.CorrecaoDados

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DuplicateHandlerJob {

  class DuplicateHandler(spark: SparkSession) extends Serializable {

    def readParquet(path: String): DataFrame = {
      spark.read
        .option("basePath", path)
        .parquet(path)
    }

    def findDuplicates(df: DataFrame, keys: Seq[String]): DataFrame = {
      df.groupBy(keys.map(col): _*)
        .count()
        .filter(col("count") > 1)
    }

    def extractDuplicateRecords(df: DataFrame, duplicates: DataFrame, keys: Seq[String]): DataFrame = {
      df.join(duplicates, keys)
    }

    def save(df: DataFrame, path: String, mode: String = "overwrite"): Unit = {
      df.write.mode(mode).parquet(path)
    }

    def groupByColumn(df: DataFrame, column: String): DataFrame = {
      df.groupBy(col(column)).count()
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Duplicate Handler Job")
      .enableHiveSupport()
      .getOrCreate()

    val path = "/datalake/prata/sources/dbms/dec/nfce/detIBS"
    val duplicatesPath = "/datalake/prata/sources/dbms/dec/nfce/det_duplicates"
    val groupedPath = "/datalake/prata/sources/dbms/dec/nfce/det_duplicates_grouped/"

    val handler = new DuplicateHandler(spark)

    // Leitura
    val df = handler.readParquet(path)
    df.printSchema()

    // Duplicados
    val duplicatesByKey = handler.findDuplicates(df, Seq("CHAVE", "nitem"))
    duplicatesByKey.show()

    val duplicatesRecords = handler.extractDuplicateRecords(df, duplicatesByKey, Seq("CHAVE", "nitem"))

    handler.save(duplicatesRecords, duplicatesPath)

    // Agrupamento por partição
    val duplicatesDF = handler.readParquet(duplicatesPath)
    duplicatesDF.printSchema()

    val groupedByPartition = handler.groupByColumn(duplicatesDF, "chave_particao")
    groupedByPartition.show()

    handler.save(groupedByPartition, groupedPath)

    spark.stop()
  }
}
//DuplicateHandlerJob.main(Array())