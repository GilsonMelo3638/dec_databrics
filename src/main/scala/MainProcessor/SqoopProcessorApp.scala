package MainProcessor

import Abstract.Principal._
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

      BPeProcessor.main(Array())
      MDFeProcessor.main(Array())
      NF3eProcessor.main(Array())
      CTeProcessor.main(Array())
      CTeSimpProcessor.main(Array())
      CTeOSProcessor.main(Array())
      GVTeProcessor.main(Array())

      // Executa o RepartitionXlmPequenosMediosProcessor como último processo
      try {
        println("=== Executando RepartitionXlmPequenosMediosProcessor ===")

        // Chamada simples sem argumentos (configurações internas)
        RepartitionXlmPequenosMediosProcessor.main(Array.empty)

        println("=== RepartitionXlmPequenosMediosProcessor concluído com sucesso ===")
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

//SqoopProcessorApp.main(Array("/datalake/bronze/sources/dbms/dec/processamento"))