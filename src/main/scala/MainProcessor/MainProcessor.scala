package MainProcessor

import DecDiarioProcessor.Principal.{InfNFCe, InfNFe, nfceDet, nfeDet}
import Extrator.{diarioNFCe, diarioNFe}
import Utils.TabelasExternas.ExternalTableCreatorMain
import org.apache.spark.sql.SparkSession

object MainProcessor {
    def main(args: Array[String]): Unit = {
        // Criar uma SparkSession compartilhada
        val spark = SparkSession.builder()
          .appName("DecUnificado")
          .config("spark.sql.parquet.writeLegacyFormat", "true")
          .config("spark.sql.debug.maxToStringFields", "100")
          .getOrCreate()

        try {
            println("=== Iniciando a extração diária de NFe ===")

            // Executando o InfNFCeProcessor
            executarProcessador(
                nome = "diáriaNFe",
                processador = diarioNFe,
                args = args
            )

            // Executando o InfNFeProcessor
            executarProcessador(
                nome = "InfNFeProcessor",
                processador = InfNFe,
                args = args
            )

            // Executando o NFeDetProcessor
            executarProcessador(
                nome = "NFeDetProcessor",
                processador = nfeDet,
                args = args
            )

            // Executando o InfNFCeProcessor
            executarProcessador(
                nome = "diáriaNFCe",
                processador = diarioNFCe,
                args = args
            )

            // Executando o InfNFCeProcessor
            executarProcessador(
                nome = "InfNFCeProcessor",
                processador = InfNFCe,
                args = args
            )

            // Executando o NFCeDetProcessor
            executarProcessador(
                nome = "NFCeDetProcessor",
                processador = nfceDet,
                args = args
            )

            // Executando o NFCeDetProcessor
            executarProcessador(
                nome = "RapartitionProcesso",
                processador = RepartitionJob.RepartitionProcessor,
                args = args
            )

            ExternalTableCreatorMain.main(Array())

            println("=== Processamento concluído com sucesso ===")
        } catch {
            case e: Exception =>
                println(s"Erro inesperado encontrado durante o processamento: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            // Finalizar a SparkSession
            spark.stop()
        }
    }

    /**
     * Função genérica para executar cada processador e capturar erros individuais.
     */
    def executarProcessador(nome: String, processador: { def main(args: Array[String]): Unit }, args: Array[String]): Unit = {
        try {
            println(s"=== Executando $nome ===")
            processador.main(args)
        } catch {
            case e: Exception =>
                println(s"Erro ao executar $nome: ${e.getMessage}")
                e.printStackTrace()
        }
    }
}