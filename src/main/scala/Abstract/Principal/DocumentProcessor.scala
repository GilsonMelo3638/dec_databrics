package Abstract.Principal

import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

abstract class DocumentProcessor(
                                  tipoDocumento: String,
                                  prataDocumento: String,
                                  nsuColumnName: String,
                                  nsuAlias: String
                                ) {

  def createSchema(): org.apache.spark.sql.types.StructType

  def generateSelectedDF(
                          parsedDF: org.apache.spark.sql.DataFrame
                        )(implicit spark: SparkSession): org.apache.spark.sql.DataFrame

  protected def createXmlDF(
                             parquetDF: org.apache.spark.sql.DataFrame
                           )(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {

    import spark.implicits._

    prataDocumento match {

      case "BPe" =>
        parquetDF
          .filter($"XML_DOCUMENTO_CLOB".isNotNull)
          .filter($"XML_DOCUMENTO_CLOB".rlike("<BPe[\\s>]"))
          .select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            col(nsuColumnName).cast("string").as(nsuAlias),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR"
          )

      case "BPeTA" =>
        parquetDF
          .filter($"XML_DOCUMENTO_CLOB".isNotNull)
          .filter($"XML_DOCUMENTO_CLOB".rlike("<BPeTA[\\s>]"))
          .select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            col(nsuColumnName).cast("string").as(nsuAlias),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR"
          )

      case "CTe" =>
        parquetDF
          .filter($"MODELO" === 57)
          .filter($"XML_DOCUMENTO_CLOB".rlike("<cteProc"))
          .select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            $"NSUSVD".cast("string").as("NSUSVD"),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR",
            $"MODELO".cast("string").as("MODELO"),
            $"TPEMIS".cast("string").as("TPEMIS")
          )

      case "CTeOS" =>
        parquetDF
          .filter($"MODELO" === 67)
          .select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            $"NSUSVD".cast("string").as("NSUSVD"),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR",
            $"MODELO".cast("string").as("MODELO"),
            $"TPEMIS".cast("string").as("TPEMIS")
          )

      case "GVTe" =>
        parquetDF
          .filter($"MODELO" === 64)
          .select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            $"NSUSVD".cast("string").as("NSUSVD"),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR",
            $"MODELO".cast("string").as("MODELO"),
            $"TPEMIS".cast("string").as("TPEMIS")
          )

      case "CTeSimp" =>
        parquetDF
          .filter($"MODELO" === 57)
          .filter($"XML_DOCUMENTO_CLOB".rlike("<cteSimpProc"))
          .select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            $"NSUSVD".cast("string").as("NSUSVD"),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR",
            $"MODELO".cast("string").as("MODELO"),
            $"TPEMIS".cast("string").as("TPEMIS")
          )

      case _ =>
        parquetDF.select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          col(nsuColumnName).cast("string").as(nsuAlias),
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR"
        )
    }
  }

  def process(spark: SparkSession): Unit = {

    import spark.implicits._

    implicit val implicitSpark: SparkSession = spark

    val schema = createSchema()

    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dataAtual = LocalDateTime.now()

    val ultimos10Dias = (1 to 10).map { diasAtras =>
      dataAtual.minus(diasAtras, ChronoUnit.DAYS).format(dateFormatter)
    }.toList

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    ultimos10Dias.foreach { dia =>

      val parquetPath = prataDocumento match {

        case "BPeTA" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_BPeTA/$dia"

        case "GVTe" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_GVTe/$dia"

        case "CTeSimp" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_CTeSimp/$dia"

        case "CTeOS" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_CTeOS/$dia"

        case _ =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$prataDocumento/processar/$dia"
      }

      val parquetPrataPath =
        s"/datalake/prata/sources/dbms/dec/$tipoDocumento/$prataDocumento"

      val parquetPathProcessado = prataDocumento match {

        case "BPe" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_BPeTA/$dia"

        case "CTe" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_CTeSimp/$dia"

        case "CTeSimp" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_CTeOS/$dia"

        case "CTeOS" =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_GVTe/$dia"

        case _ =>
          s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processado/$dia"
      }

      if (fs.exists(new Path(parquetPath))) {

        val startTime = LocalDateTime.now()

        println(s"Início da iteração para o dia $dia: $startTime")
        println(s"Lendo dados do caminho: $parquetPath")
        println(s"Caminho de gravação dos dados: $parquetPrataPath")

        try {

          val parquetDF = spark.read.parquet(parquetPath)

          val totalCount = parquetDF.count()

          val distinctCount =
            parquetDF
              .select(col(nsuColumnName))
              .distinct()
              .count()

          if (totalCount != distinctCount) {

            println(
              s"Erro: Total de registros ($totalCount) é diferente do total de valores distintos da coluna $nsuColumnName ($distinctCount) no caminho: $parquetPath"
            )

            throw new IllegalStateException(
              s"Inconsistência nos dados: existem registros duplicados pela coluna $nsuColumnName."
            )

          } else {

            println(
              s"Verificação bem-sucedida: Total ($totalCount) e distintos da coluna $nsuColumnName ($distinctCount) são iguais no caminho: $parquetPath"
            )
          }

          val xmlDF = createXmlDF(parquetDF)

          val parsedDF =
            xmlDF.withColumn(
              "parsed",
              from_xml($"xml", schema)
            )

          val selectedDF = generateSelectedDF(parsedDF)

          val selectedDFComParticao =
            selectedDF.withColumn(
              "chave_particao",
              substring(col("chave"), 3, 4)
            )

          val repartitionedDF =
            selectedDFComParticao.repartition(5)

          repartitionedDF
            .write
            .mode("append")
            .format("parquet")
            .option("compression", "lz4")
            .option("parquet.block.size", 500 * 1024 * 1024)
            .partitionBy("chave_particao")
            .save(parquetPrataPath)

          println(s"Gravação concluída para $dia")

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

            println(
              s"Arquivos movidos de $parquetPath para $parquetPathProcessado com sucesso."
            )
          }

          val saveEndTime = LocalDateTime.now()

          println(s"Gravação concluída: $saveEndTime")

        } catch {

          case e: Exception =>

            println(s"Erro ao processar o dia $dia: ${e.getMessage}")
            e.printStackTrace()
        }

      } else {

        println(s"Diretório não encontrado para o dia $dia: $parquetPath")
      }
    }
  }
}

object BPeProcessor
  extends DocumentProcessor("bpe", "BPe", "NSU", "NSU") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.BPeSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.BPeProcessor.generateSelectedDF(parsedDF)
  }
}

object BPeTAProcessor
  extends DocumentProcessor("bpe", "BPeTA", "NSU", "NSU") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.BPeTASchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.BPeTAProcessor.generateSelectedDF(parsedDF)
  }
}

object MDFeProcessor
  extends DocumentProcessor("mdfe", "MDFe", "NSU", "NSU") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.MDFeSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.MDFeProcessor.generateSelectedDF(parsedDF)
  }
}

object CTeProcessor
  extends DocumentProcessor("cte", "CTe", "NSUSVD", "NSUSVD") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.CTeSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.CTeProcessor.generateSelectedDF(parsedDF)
  }
}

object CTeOSProcessor
  extends DocumentProcessor("cte", "CTeOS", "NSUSVD", "NSUSVD") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.CTeOSSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.CTeOSProcessor.generateSelectedDF(parsedDF)
  }
}

object GVTeProcessor
  extends DocumentProcessor("cte", "GVTe", "NSUSVD", "NSUSVD") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.GVTeSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.GVTeProcessor.generateSelectedDF(parsedDF)
  }
}

object CTeSimpProcessor
  extends DocumentProcessor("cte", "CTeSimp", "NSUSVD", "NSUSVD") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.CTeSimpSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.CTeSimpProcessor.generateSelectedDF(parsedDF)
  }
}

object NF3eProcessor
  extends DocumentProcessor("nf3e", "NF3e", "NSU", "NSU") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.NF3eSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.NF3eProcessor.generateSelectedDF(parsedDF)
  }
}

object NFComProcessor
  extends DocumentProcessor("nfcom", "NFCom", "NSU", "NSU") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.NFComSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.NFComProcessor.generateSelectedDF(parsedDF)
  }
}

object NFeProcessor
  extends DocumentProcessor("nfe", "NFe", "NSUDF", "NSUDF") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.NFeSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.NFeProcessor.generateSelectedDF(parsedDF)
  }
}

object NFCeProcessor
  extends DocumentProcessor("nfce", "NFCe", "NSU", "NSU") {

  override def createSchema()
  : org.apache.spark.sql.types.StructType =
    Schemas.NFCeSchema.createSchema()

  override def generateSelectedDF(
                                   parsedDF: org.apache.spark.sql.DataFrame
                                 )(implicit spark: SparkSession)
  : org.apache.spark.sql.DataFrame = {

    Processors.NFCeProcessor.generateSelectedDF(parsedDF)
  }
}