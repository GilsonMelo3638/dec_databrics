package MainProcessor

import RepartitionJob.RepartitionXlmPequenosMediosProcessor
import Sqoop.SqoopProcessor
import DECBPeProcessor.BpeProcDiarioProcessor
import DECMDFeProcessor.MDFeProcDiarioProcessor
import DECNF3eProcessor.NF3eProcDiarioProcessor
import DECCTeProcessor.CTeProcDiarioProcessor
import DECCTeOSProcessor.CTeOSDiarioProcessor
import DECGVTeProcessor.GVTeDiarioProcessor
import org.apache.spark.sql.SparkSession

import java.util.Properties

object SqoopProcessorApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: SqoopProcessorApp <targetDirBase>")
      System.exit(1)
    }

    val targetDirBase = args(0) // Caminho base de destino no HDFS

    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("SqoopToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    try {
      println("=== Iniciando o SqoopProcessorApp ===")

      // Configurações de conexão com o banco de dados Oracle
      val defaultJdbcUrl = "jdbc:oracle:thin:@sefsrvprd704.fazenda.net:1521/ORAPRD21"
      val defaultConnectionProperties = new Properties()
      defaultConnectionProperties.put("user", "userdec")
      defaultConnectionProperties.put("password", "userdec201811")
      defaultConnectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

      // Configurações de conexão específicas para BPe
      val oraprd23JdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
      val oraprd23ConnectionProperties = new Properties()
      oraprd23ConnectionProperties.put("user", "admhadoop")
      oraprd23ConnectionProperties.put("password", ".admhadoop#")
      oraprd23ConnectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

      // Lista de tipos de documentos e suas colunas de particionamento
      val documentos = List(
        ("BPe", "NSU"),
        ("CTe", "NSUSVD"),
        ("MDFe", "NSU"),
        ("NF3e", "NSU")
      )

      // Executa os processadores para cada tipo de documento
      documentos.foreach { case (documentType, splitByColumn) =>
        try {
          println(s"=== Processando $documentType ===")

          // Escolhe as configurações de conexão com base no tipo de documento
          val (jdbcUrl, connectionProperties) = if (documentType == "CTe") {
            (defaultJdbcUrl, defaultConnectionProperties)
          } else {
            (oraprd23JdbcUrl, oraprd23ConnectionProperties)
          }

          SqoopProcessor.processDocument(spark, jdbcUrl, connectionProperties, documentType, splitByColumn, targetDirBase)
        } catch {
          case e: Exception =>
            println(s"Erro ao processar $documentType: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      // Executa as classes de processamento diário após as queries
      try {
        println("=== Executando BpeProcDiarioProcessor ===")
        BpeProcDiarioProcessor.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar BpeProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando MDFeProcDiarioProcessor ===")
        MDFeProcDiarioProcessor.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar MDFeProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando NF3eProcDiarioProcessor ===")
        NF3eProcDiarioProcessor.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar NF3eProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando CTeProcDiarioProcessor ===")
        CTeProcDiarioProcessor.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar CTeProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando CTeOSDiarioProcessor ===")
        CTeOSDiarioProcessor.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar CTeOSDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando GVTeDiarioProcessor ===")
        GVTeDiarioProcessor.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar GVTeDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      // Executa o RepartitionXlmPequenosMediosProcessor como último processo
      try {
        println("=== Executando RepartitionXlmPequenosMediosProcessor ===")
        val repartitionProcessor = new RepartitionXlmPequenosMediosProcessor(spark)

        // Define os caminhos e configurações para cada tipo de documento
        val configs = Map(
          "CTe" -> ("/datalake/prata/sources/dbms/dec/cte/CTe", 10, 10),
          "CTeOS" -> ("/datalake/prata/sources/dbms/dec/cte/CTeOS", 2, 2),
          "GVTe" -> ("/datalake/prata/sources/dbms/dec/cte/GVTe", 2, 2),
          "BPe" -> ("/datalake/prata/sources/dbms/dec/bpe/BPe", 5, 5),
          "MDFe" -> ("/datalake/prata/sources/dbms/dec/mdfe/MDFe", 4, 4),
          "NF3e" -> ("/datalake/prata/sources/dbms/dec/nf3e/nf3e", 4, 4)
        )

        // Processa cada tipo de documento
        configs.foreach { case (docType, (basePath, maxFiles, targetRepartition)) =>
          println(s"Processando $docType...")
          repartitionProcessor.processPartitions(basePath, maxFiles, targetRepartition)
          println(s"Concluído processamento de $docType.")
        }
      } catch {
        case e: Exception =>
          println(s"Erro ao executar RepartitionXlmPequenosMediosProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      println("=== SqoopProcessorApp concluído com sucesso ===")
    } catch {
      case e: Exception =>
        println(s"Erro inesperado durante a execução do SqoopProcessorApp: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop() // Fecha a sessão do Spark ao final de tudo
    }
  }
}