package Utils.Auditoria

import org.apache.spark.sql.{DataFrame, SparkSession}

class ExcluirDuplicidade(
                           spark: SparkSession,
                           basePath: String = "/datalake/prata/sources/dbms/dec"
                         ) {

  /**
   * Remove duplicidades de uma partição específica
   */
  def processPartition(
                        tipoArquivo: String,
                        tipoInformacao: String,
                        chaveParticao: String,
                        colunaDedupe: String = "chave",
                        reparticoes: Int = 10,
                        sufixoDestino: String = "_2"
                      ): Unit = {

    try {

      val inputPath =
        s"$basePath/$tipoArquivo/$tipoInformacao/chave_particao=$chaveParticao"

      val outputPath =
        s"$basePath/$tipoArquivo/$tipoInformacao/chave_particao=${chaveParticao}$sufixoDestino"

      println(s"Lendo dados de: $inputPath")

      val df = spark.read.format("parquet").load(inputPath)

      val dfDeduplicado = df.dropDuplicates(colunaDedupe)

      println(s"Gravando dados deduplicados em: $outputPath")

      dfDeduplicado
        .repartition(reparticoes)
        .write
        .format("parquet")
        .option("compression", "lz4")
        .mode("overwrite")
        .save(outputPath)

      println(s"Partição $chaveParticao processada com sucesso.")

    } catch {
      case e: Exception =>
        println(
          s"Erro ao processar a partição $chaveParticao: ${e.getMessage}"
        )
        throw e
    }
  }

  /**
   * Processa múltiplas partições
   */
  def processPartitions(
                         tipoArquivo: String,
                         tipoInformacao: String,
                         particoes: Seq[String],
                         colunaDedupe: String = "chave",
                         reparticoes: Int = 10,
                         sufixoDestino: String = "_2"
                       ): Unit = {

    particoes.foreach { particao =>
      processPartition(
        tipoArquivo,
        tipoInformacao,
        particao,
        colunaDedupe,
        reparticoes,
        sufixoDestino
      )
    }
  }
}


//val spark = SparkSession.builder().appName("Remover Duplicidades").getOrCreate()
//
//val deduplicator = new ExcluirDuplicidade(spark)
//
//val particoes = Seq("2502", "2503")
//
//deduplicator.processPartitions(
//  tipoArquivo = "cte",
//  tipoInformacao = "GVTe",
//  particoes = particoes,
//  colunaDedupe = "chave",
//  reparticoes = 1,
//  sufixoDestino = "_2"
//)

