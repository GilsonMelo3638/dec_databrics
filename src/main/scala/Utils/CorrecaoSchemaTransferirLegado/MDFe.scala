package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MDFe {
  def main(args: Array[String]): Unit = {
    // Configuração do Spark
    val spark = SparkSession.builder()
      .appName("NFCeProcessor")
      .getOrCreate()

    // Configuração de compressão
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    // Caminhos de entrada e saída
    val inputPath = "/datalake/bronze/sources/dbms/dec/diario/mdfe/year=2025/month=04"
    val outputPath = "/datalake/bronze/sources/dbms/legado/dec/mdfe_diario/"

    // Número de partições (ajuste conforme necessário)
    val numPartitions = 2

    try {
      // Ler os dados de entrada
      val df = spark.read.parquet(inputPath)

      // Converter tipos de colunas e renomear a coluna day existente
      val convertedDF = convertDataTypes(df)

      // Processar e particionar os dados
      processAndPartitionData(convertedDF, outputPath, numPartitions)

    } finally {
//      spark.stop()
    }
  }

  // Função para converter tipos de colunas
  def convertDataTypes(df: DataFrame): DataFrame = {
    df.withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
      .withColumnRenamed("day", "original_day") // Renomeia a coluna day existente
  }

  // Função para processar e particionar os dados
  def processAndPartitionData(df: DataFrame, outputPath: String, numPartitions: Int): Unit = {
    // Adicionar colunas temporárias para partição
    val partitionedDF = df.withColumn("year_temp", substring(col("DHPROC"), 7, 4))
      .withColumn("month_temp", substring(col("DHPROC"), 4, 2))
      .withColumn("day_temp", substring(col("DHPROC"), 1, 2).cast(IntegerType))

    // Obter anos, meses e dias distintos para processamento
    val dateCombinations = partitionedDF.select("year_temp", "month_temp", "day_temp").distinct().collect()

    // Processar cada combinação de data
    dateCombinations.foreach { row =>
      val year = row.getString(0)
      val month = row.getString(1)
      val dayNum = row.getInt(2)

      // Filtrar dados para o dia específico e remover colunas temporárias
      val dayDF = partitionedDF.filter(
        col("year_temp") === year &&
          col("month_temp") === month &&
          col("day_temp") === dayNum
      ).drop("year_temp", "month_temp", "day_temp", "original_day")

      // Escrever os dados particionados (sem incluir as colunas no schema)
      dayDF.repartition(numPartitions)
        .write
        .mode("append")
        .option("compression", "lz4")
        .parquet(s"$outputPath/year=$year/month=$month/day=$dayNum")
    }
  }
}
//MDFe.main(Array())