package Utils.CorrecaoDados

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.hadoop.fs.{FileSystem, Path}

object DocumentosFaltantes {
  // Configurações de caminho
  private val BaseDatalakePath = "/datalake/"
  private val SourcePath = "sources/dbms/dec/"
  private val NfcePath = "nfce/"

  private val Paths = Map(
    "bronze" -> s"${BaseDatalakePath}bronze/${SourcePath}${NfcePath}",
    "prata" -> s"${BaseDatalakePath}prata/${SourcePath}${NfcePath}infNFCeV2/",
    "faltantes" -> s"${BaseDatalakePath}bronze/${SourcePath}processamento/${NfcePath}faltantes/"
  )

  // Configurações de período
  private val PeriodRange = Map(
    "startYear" -> 2025,
    "startMonth" -> 1,
    "endYear" -> 2025,
    "endMonth" -> 1
  )

  def main(args: Array[String]): Unit = {
    // Cria a sessão do Spark
    val spark = SparkSession.builder()
      .appName("NFCE Processor")
      .getOrCreate()

    try {
      // Configura a compactação
      spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

      // Carrega o DataFrame do prata
      val prataDF = loadPrataData(spark)

      // Processa cada mês no período
      processDateRange(spark, prataDF)

      println("Processamento concluído com sucesso!")
    } finally {
      spark.stop()
    }
  }

  private def loadPrataData(spark: SparkSession): DataFrame = {
    spark.read.parquet(Paths("prata"))
      .select(lower(col("chave")).alias("chave"))
      .distinct()
  }

  private def processDateRange(spark: SparkSession, prataDF: DataFrame): Unit = {
    val (startYear, startMonth, endYear, endMonth) = (
      PeriodRange("startYear"),
      PeriodRange("startMonth"),
      PeriodRange("endYear"),
      PeriodRange("endMonth")
    )

    for {
      year <- startYear to endYear
      month <- getMonthRange(year, startYear, startMonth, endYear, endMonth)
    } {
      processMonth(spark, prataDF, year, month)
    }
  }

  private def getMonthRange(year: Int, startYear: Int, startMonth: Int,
                            endYear: Int, endMonth: Int): Range = {
    if (year == startYear && year == endYear) startMonth to endMonth
    else if (year == startYear) startMonth to 12
    else if (year == endYear) 1 to endMonth
    else 1 to 12
  }

  private def processMonth(spark: SparkSession, prataDF: DataFrame, year: Int, month: Int): Unit = {
    val yearMonth = f"$year$month%02d"
    val bronzePath = s"${Paths("bronze")}$yearMonth"
    val outputPath = s"${Paths("faltantes")}$yearMonth"

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (!fs.exists(new Path(bronzePath))) {
      println(s"[INFO] Caminho $bronzePath não encontrado. Pulando...")
      return
    }

    val bronzeDF = spark.read.parquet(bronzePath)
      .withColumn("chave", lower(col("CHAVE")))

    val missingKeysDF = bronzeDF.join(prataDF, Seq("chave"), "left_anti")

    if (missingKeysDF.isEmpty) {
      println(s"[INFO] Nenhuma chave faltante encontrada para $yearMonth")
      return
    }

    saveMissingKeys(missingKeysDF, outputPath)
    println(s"[SUCCESS] Chaves faltantes para $yearMonth salvas em: $outputPath")
  }

  private def saveMissingKeys(df: DataFrame, path: String): Unit = {
    df.repartition(10)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "lz4")
      .save(path)
  }
}
// DocumentosFaltantes.main(Array())