package Abstract.Cancelamento

import Processors.{BPeEventoProcessor, MDFeEventoProcessor, NF3eEventoProcessor, NFCeEventoProcessor, NFeEventoProcessor, CTeEventoProcessor}
import Schemas.{BPeEventoSchema, MDFeEventoSchema, NF3eEventoSchema, NFCeEventoSchema, NFeEventoSchema, CTeEventoSchema}
import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

// Classe genérica para processamento de documentos
abstract class DecCancelamentoDiarioProcessor(
                                               val tipoDocumento: String,
                                               val tipoDocumentoCancelamento: String,
                                               val prataDocumento: String,
                                               val schema: org.apache.spark.sql.types.StructType
                                             ) {

  def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(s"Extract${tipoDocumento.capitalize}Cancelamento").enableHiveSupport().getOrCreate()

    // Importação dos implicits do Spark
    import spark.implicits._

    // Gerar a lista dos últimos 10 dias no formato YYYYMMDD, começando do dia anterior
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dataAtual = LocalDateTime.now()
    val ultimos10Dias = (1 to 16).map { diasAtras =>
      dataAtual.minus(diasAtras, ChronoUnit.DAYS).format(dateFormatter)
    }.toList

    // Obter o FileSystem do Hadoop para verificar a existência dos diretórios
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    ultimos10Dias.foreach { dia =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumentoCancelamento/processar/$dia"
      val parquetPrataPath = s"/datalake/prata/sources/dbms/dec/$tipoDocumento/$prataDocumento"
      val parquetPathProcessado = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumentoCancelamento/processado/$dia"

      // Verificar se o diretório existe
      if (fs.exists(new Path(parquetPath))) {
        // Registrar o horário de início da iteração
        val startTime = LocalDateTime.now()
        println(s"Início da iteração para o dia $dia: $startTime")
        println(s"Lendo dados do caminho: $parquetPath")
        println(s"Caminho de gravação dos dados: $parquetPrataPath")

        try {
          // 1. Carrega o arquivo Parquet
          val parquetDF = spark.read.parquet(parquetPath)

          // Verificação de consistência entre total de registros e registros distintos
          val totalCount = parquetDF.count()
          val distinctCount = parquetDF.distinct().count()

          if (totalCount != distinctCount) {
            println(s"Erro: Total de registros ($totalCount) é diferente do total de registros distintos ($distinctCount) no caminho: $parquetPath")
            throw new IllegalStateException("Inconsistência nos dados: total e distinto não coincidem.")
          } else {
            println(s"Verificação bem-sucedida: Total ($totalCount) e distintos ($distinctCount) são iguais no caminho: $parquetPath")
          }

          // 2. Seleciona as colunas
          val xmlDF = selectColumns(parquetDF)

          // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
          val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))

          // 4. Gera o DataFrame selectedDF usando a nova classe
          implicit val sparkSession: SparkSession = spark
          val selectedDF = generateSelectedDF(parsedDF)
          val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

          // Redistribuir os dados para 5 partições
          val repartitionedDF = selectedDFComParticao.repartition(2)

          // Escrever os dados particionados
          repartitionedDF
            .write.mode("append")
            .format("parquet")
            .option("compression", "lz4")
            .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
            .partitionBy("chave_particao") // Garante a separação por partição
            .save(parquetPrataPath)

          println(s"Gravação concluída para $dia")

          // Mover os arquivos para a pasta processada
          val srcPath = new Path(parquetPath)
          if (fs.exists(srcPath)) {
            val destPath = new Path(parquetPathProcessado)
            if (!fs.exists(destPath)) {
              fs.mkdirs(destPath)
            }
            fs.listStatus(srcPath).foreach { fileStatus =>
              val srcFile = fileStatus.getPath
              val destFile = new Path(destPath, srcFile.getName)
              fs.rename(srcFile, destFile)
            }
            fs.delete(srcPath, true)
            println(s"Arquivos movidos de $parquetPath para $parquetPathProcessado com sucesso.")
          }

          // Registrar o horário de término da gravação
          val saveEndTime = LocalDateTime.now()
          println(s"Gravação concluída: $saveEndTime")
        } catch {
          case e: Exception =>
            println(s"Erro ao processar o dia $dia: ${e.getMessage}")
        }
      } else {
        println(s"Diretório não encontrado para o dia $dia: $parquetPath")
      }
    }
  }

  // Método para selecionar colunas específicas de cada tipo de documento
  def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame
}

// Implementações específicas para cada tipo de documento

object BPe extends DecCancelamentoDiarioProcessor(
  "bpe", "bpe_cancelamento", "cancelamento", BPeEventoSchema.createSchema()
) {
  override def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Importação dos implicits do Spark
    import parquetDF.sparkSession.implicits._
    parquetDF.select(
      $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
      $"NSU".cast("string").as("NSU"),
      $"DHPROC",
      $"DHEVENTO",
      $"IP_TRANSMISSOR"
    )
  }

  override def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    BPeEventoProcessor.generateSelectedDF(parsedDF)
  }
}

object MDFe extends DecCancelamentoDiarioProcessor(
  "mdfe", "mdfe_cancelamento", "cancelamento", MDFeEventoSchema.createSchema()
) {
  override def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Importação dos implicits do Spark
    import parquetDF.sparkSession.implicits._
    parquetDF.select(
      $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
      $"NSU".cast("string").as("NSU"),
      $"DHPROC",
      $"DHEVENTO",
      $"IP_TRANSMISSOR"
    )
  }

  override def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    MDFeEventoProcessor.generateSelectedDF(parsedDF)
  }
}

object CTe extends DecCancelamentoDiarioProcessor(
  "cte", "cte_cancelamento", "cancelamento", CTeEventoSchema.createSchema()
) {
  override def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Importação dos implicits do Spark
    import parquetDF.sparkSession.implicits._
    parquetDF.select(
      $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
      $"NSUSVD".cast("string").as("NSUSVD"),
      $"DHPROC",
      $"DHEVENTO",
      $"IP_TRANSMISSOR"
    )
  }

  override def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    CTeEventoProcessor.generateSelectedDF(parsedDF)
  }
}

object NF3e extends DecCancelamentoDiarioProcessor(
  "nf3e", "nf3e_cancelamento", "cancelamento", NF3eEventoSchema.createSchema()
) {
  override def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Importação dos implicits do Spark
    import parquetDF.sparkSession.implicits._
    parquetDF.select(
      $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
      $"NSU".cast("string").as("NSU"),
      $"DHPROC",
      $"DHEVENTO",
      $"IP_TRANSMISSOR"
    )
  }

  override def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    NF3eEventoProcessor.generateSelectedDF(parsedDF)
  }
}

object NFCe extends DecCancelamentoDiarioProcessor(
  "nfce", "nfce_cancelamento", "cancelamento", NFCeEventoSchema.createSchema()
) {
  override def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Importação dos implicits do Spark
    import parquetDF.sparkSession.implicits._
    parquetDF.select(
      $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
      $"NSU".cast("string").as("NSU"),
      $"DHPROC",
      $"DHEVENTO",
      $"IP_TRANSMISSOR"
    )
  }

  override def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    NFCeEventoProcessor.generateSelectedDF(parsedDF)
  }
}

object NFe extends DecCancelamentoDiarioProcessor(
  "nfe", "nfe_cancelamento", "cancelamento", NFeEventoSchema.createSchema()
) {
  override def selectColumns(parquetDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Importação dos implicits do Spark
    import parquetDF.sparkSession.implicits._
    parquetDF.select(
      $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
      $"NSUDF".cast("string").as("NSUDF"), // Coluna específica da NFe
      $"DHPROC",
      $"DHEVENTO",
      $"IP_TRANSMISSOR"
    )
  }

  override def generateSelectedDF(parsedDF: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    NFeEventoProcessor.generateSelectedDF(parsedDF)
  }
}
// Exemplo de uso
// BPe.main(Array())
// NF3e.main(Array())
// NFCe.main(Array())
// NFe.main(Array())