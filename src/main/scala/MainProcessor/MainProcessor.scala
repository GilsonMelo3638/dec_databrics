package MainProcessor

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
            println("=== Iniciando o SqoopNFCeProcessor ===")

            // Executando o InfNFCeProcessor
            executarProcessador(
                nome = "SqoopNFeProcessor",
                processador = Sqoop.SqoopNFeProcessor,
                args = args
            )

            // Executando o InfNFCeProcessor
            executarProcessador(
                nome = "SqoopNFCeProcessor",
                processador = Sqoop.SqoopNFCeProcessor,
                args = args
            )

            // Executando o InfNFeProcessor
            executarProcessador(
                nome = "InfNFeProcessor",
                processador = DECInfNFeJob.InfNFeProcessor,
                args = args
            )

            // Executando o NFeDetProcessor
            executarProcessador(
                nome = "NFeDetProcessor",
                processador = DECNFeDetJob.NFeDetProcessor,
                args = args
            )

            // Executando o InfNFCeProcessor
            executarProcessador(
                nome = "InfNFCeProcessor",
                processador = DECInfNFCeJob.InfNFCeProcessor,
                args = args
            )

            // Executando o NFCeDetProcessor
            executarProcessador(
                nome = "NFCeDetProcessor",
                processador = DECNFCeDetJob.NFCeDetProcessor,
                args = args
            )

            // Executando o NFCeDetProcessor
            executarProcessador(
                nome = "RapartitionProcesso",
                processador = RepartitionJob.RepartitionProcessor,
                args = args
            )

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
