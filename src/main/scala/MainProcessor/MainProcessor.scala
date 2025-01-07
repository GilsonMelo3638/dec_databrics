package MainProcessor

object MainProcessor {
    def main(args: Array[String]): Unit = {
        try {
            println("=== Iniciando o processamento ===")

            // Executando o InfNFeProcessor
            println("=== Executando InfNFeProcessor ===")
            DECInfNFeJob.InfNFeProcessor.main(args)

            // Executando o NFeDetProcessor
            println("=== Executando NFeDetProcessor ===")
            DECNFeDetJob.NFeDetProcessor.main(args)

            // Executando o InfNFCeProcessor
            println("=== Executando InfNFeProcessor ===")
            DECInfNFCeJob.InfNFCeProcessor.main(args)

            // Executando o NFeDetProcessor
            println("=== Executando NFeDetProcessor ===")
            DECNFCeDetJob.NFCeDetProcessor.main(args)



            println("=== Processamento concluído com sucesso ===")
        } catch {
            case e: Exception =>
                println(s"Erro encontrado durante o processamento: ${e.getMessage}")
                e.printStackTrace()
        }
    }
}
