package MainProcessor

import DecDiarioProcessor.Principal.{Bpe, CTeOS, CTe, CTeSimp, GVTe, MDFe, NF3e}
import Extrator.diarioGenerico
import RepartitionJob.RepartitionXlmPequenosMediosProcessor
import org.apache.spark.sql.SparkSession

import java.util.Properties

object SqoopProcessorApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: ExtratorProcessorApp <targetDirBase>")
      System.exit(1)
    }

    val targetDirBase = args(0) // Caminho base de destino no HDFS

    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("ExtratorToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    try {
      println("=== Iniciando o ExtratorProcessorApp ===")

      // Configurações de conexão com o banco de dados Oracle para ORAPRD23
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

          // Usa sempre a conexão com ORAPRD23
          diarioGenerico.processDocument(spark, oraprd23JdbcUrl, oraprd23ConnectionProperties, documentType, splitByColumn, targetDirBase)
        } catch {
          case e: Exception =>
            println(s"Erro ao processar $documentType: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      // Executa as classes de processamento diário após as queries
      try {
        println("=== Executando BpeProcDiarioProcessor ===")
        Bpe.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar BpeProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando MDFeProcDiarioProcessor ===")
        MDFe.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar MDFeProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando NF3eProcDiarioProcessor ===")
        NF3e.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar NF3eProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando CTeProcDiarioProcessor ===")
        CTe.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar CTeProcDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando CTeSimpDiarioProcessor ===")
        CTeSimp.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar CTeSimpDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando CTeOsDiarioProcessor ===")
        CTeOS.main(Array())
      } catch {
        case e: Exception =>
          println(s"Erro ao executar CTeOsDiarioProcessor: ${e.getMessage}")
          e.printStackTrace()
      }

      try {
        println("=== Executando GVTeDiarioProcessor ===")
        GVTe.main(Array())
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
          "CTeSimp" -> ("/datalake/prata/sources/dbms/dec/cte/CTeOS", 2, 2),
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

      println("=== ExtratorProcessorApp concluído com sucesso ===")
    } catch {
      case e: Exception =>
        println(s"Erro inesperado durante a execução do ExtratorProcessorApp: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop() // Fecha a sessão do Spark ao final de tudo
    }
  }
}