package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.{LocalDate, ZoneId}

abstract class DocumentProcessor {
  // Configurações específicas para cada tipo de documento
  def documentType: String
  def defaultPartitions: Int
  def inputBasePath: String = "/datalake/bronze/sources/dbms/dec/diario"
  def outputBasePath: String = "/datalake/bronze/sources/dbms/legado/dec"

  // Método abstrato para conversão de tipos de colunas
  def convertDataTypes(df: DataFrame): DataFrame = {
    df.withColumnRenamed("day", "original_day") // Comportamento padrão para todos
  }

  // Método principal que pode ser chamado via main
  def main(args: Array[String]): Unit = {
    // Obter mês e ano anterior
    val previousMonthDate = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusMonths(1)
    var year = previousMonthDate.getYear
    var month = previousMonthDate.getMonthValue
    var numPartitions = defaultPartitions

    // Processar argumentos se fornecidos (sobrescrevem os valores padrão)
    if (args.length >= 2) {
      year = args(0).toInt
      month = args(1).toInt
    }
    if (args.length >= 3) {
      numPartitions = args(2).toInt
    }

    process(year, month, Some(numPartitions))
  }

  // Método de processamento principal
  def process(year: Int, month: Int, numPartitions: Option[Int] = None): Unit = {
    val spark = SparkSession.builder()
      .appName(s"${documentType}Processor")
      .getOrCreate()

    try {
      // Configuração de compressão
      spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

      // Caminhos de entrada e saída
      val inputPath = s"$inputBasePath/$documentType/year=$year/month=${month.formatted("%02d")}"
      val outputPath = s"$outputBasePath/${documentType}_diario/"

      // Número de partições
      val partitions = numPartitions.getOrElse(defaultPartitions)

      // Verificar se o caminho de entrada existe
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val inputPathObj = new org.apache.hadoop.fs.Path(inputPath)

      if (fs.exists(inputPathObj)) {
        // Ler e processar os dados
        val df = spark.read.parquet(inputPath)
        val convertedDF = convertDataTypes(df)
        processAndPartitionData(convertedDF, outputPath, partitions)

        println(s"Processamento concluído para $documentType - $year-${month.formatted("%02d")}")
      } else {
        println(s"Caminho de entrada não encontrado: $inputPath")
        println(s"Pulando processamento para $documentType - $year-${month.formatted("%02d")}")
      }

    } finally {
      spark.stop()
    }
  }

  // Método genérico para processamento e particionamento
  protected def processAndPartitionData(df: DataFrame, outputPath: String, numPartitions: Int): Unit = {
    val partitionedDF = df.withColumn("year_temp", substring(col("DHPROC"), 7, 4))
      .withColumn("month_temp", substring(col("DHPROC"), 4, 2))
      .withColumn("day_temp", substring(col("DHPROC"), 1, 2).cast(IntegerType))

    val dateCombinations = partitionedDF.select("year_temp", "month_temp", "day_temp").distinct().collect()

    dateCombinations.foreach { row =>
      val year = row.getString(0)
      val month = row.getString(1)
      val dayNum = row.getInt(2)

      val dayDF = partitionedDF.filter(
        col("year_temp") === year &&
          col("month_temp") === month &&
          col("day_temp") === dayNum
      ).drop("year_temp", "month_temp", "day_temp", "original_day")

      dayDF.repartition(numPartitions)
        .write
        .mode("append")
        .option("compression", "lz4")
        .parquet(s"$outputPath/year=$year/month=$month/day=$dayNum")
    }
  }
}

// Implementações específicas para cada tipo de documento (mantidas iguais)

object MDFeEvento extends DocumentProcessor {
  override def documentType: String = "mdfe_evento"
  override def defaultPartitions: Int = 2

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NF3eEvento extends DocumentProcessor {
  override def documentType: String = "nf3e_evento"
  override def defaultPartitions: Int = 2

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NFComEvento extends DocumentProcessor {
  override def documentType: String = "nfcom_evento"
  override def defaultPartitions: Int = 2

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NFeEvento extends DocumentProcessor {
  override def documentType: String = "nfe_evento"
  override def defaultPartitions: Int = 5

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSUDF", col("NSUDF").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NFeCancelamento extends DocumentProcessor {
  override def documentType: String = "nfe_cancelamento"
  override def defaultPartitions: Int = 1

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSUDF", col("NSUDF").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NFCeCancelamento extends DocumentProcessor {
  override def documentType: String = "nfce_cancelamento"
  override def defaultPartitions: Int = 2

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object BPeCancelamento extends DocumentProcessor {
  override def documentType: String = "bpe_cancelamento"
  override def defaultPartitions: Int = 1

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NFComCancelamento extends DocumentProcessor {
  override def documentType: String = "nfcom_cancelamento"
  override def defaultPartitions: Int = 1

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object NF3eCancelamento extends DocumentProcessor {
  override def documentType: String = "nf3e_cancelamento"
  override def defaultPartitions: Int = 1

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object CTeCancelamento extends DocumentProcessor {
  override def documentType: String = "cte_cancelamento"
  override def defaultPartitions: Int = 1

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSUSVD", col("NSUSVD").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}

object MDFeCancelamento extends DocumentProcessor {
  override def documentType: String = "mdfe_cancelamento"
  override def defaultPartitions: Int = 1

  override def convertDataTypes(df: DataFrame): DataFrame = {
    super.convertDataTypes(df)
      .withColumn("NSU", col("NSU").cast(StringType))
      .withColumn("CSTAT", col("CSTAT").cast(StringType))
  }
}


// Exemplo de como executar (agora sem parâmetros para usar o mês anterior):

MDFeEvento.main(Array())      // Usa mês anterior automaticamente
NF3eEvento.main(Array())      // Usa mês anterior automaticamente
NFComEvento.main(Array())     // Usa mês anterior automaticamente
NFeEvento.main(Array())       // Usa mês anterior automaticamente
NFeCancelamento.main(Array())       // Usa mês anterior automaticamente
NFCeCancelamento.main(Array())       // Usa mês anterior automaticamente
BPeCancelamento.main(Array())       // Usa mês anterior automaticamente
NFComCancelamento.main(Array())       // Usa mês anterior automaticamente
NF3eCancelamento.main(Array())       // Usa mês anterior automaticamente
CTeCancelamento.main(Array())       // Usa mês anterior automaticamente
MDFeCancelamento.main(Array())       // Usa mês anterior automaticamente

// Ou ainda pode sobrescrever com parâmetros específicos:
// BPe.main(Array("2025", "6")) // Força ano 2025, mês 6
// CTe.main(Array("2025", "6", "5")) // Força ano 2025, mês 6, com 5 partições