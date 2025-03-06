package MainProcessor
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
      val jdbcUrl = "jdbc:oracle:thin:@sefsrvprd704.fazenda.net:1521/ORAPRD21"
      val connectionProperties = new Properties()
      connectionProperties.put("user", "userdec")
      connectionProperties.put("password", "userdec201811")
      connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

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