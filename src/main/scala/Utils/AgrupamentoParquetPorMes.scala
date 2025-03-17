package Utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object AgrupamentoParquetPorMes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Agrupamento Parquet por Mês")
      .getOrCreate()

    // Definir os tipos de documentos e seus diretórios
    val tiposDocumentos = Seq("bpe", "cte", "mdfe", "nf3e", "nfe_cancelamento", "nfce_cancelamento" , "nf3e_cancelamento", "bpe_cancelamento", "nfe",  "nfce"  )

    // Diretório base de origem e destino
    val baseOrigem = "/datalake/bronze/sources/dbms/dec/processamento/"
    val baseDestino = "/datalake/bronze/sources/dbms/dec/"

    // Mês e ano de interesse no formato YYYYMM
    val anoMes = "202503"

    // Tamanho máximo por partição (300 MB)
    val tamanhoMaximoParticaoMB = 300

    // Função para calcular o número de partições com base no tamanho dos dados
    def calcularNumParticoes(df: DataFrame): Int = {
      // Obtém o tamanho aproximado dos dados em bytes
      val tamanhoBytes = df.queryExecution.optimizedPlan.stats.sizeInBytes.toLong // Converte BigInt para Long

      // Converte o tamanho para MB
      val tamanhoMB = tamanhoBytes / (1024 * 1024)

      // Calcula o número de partições necessário
      val numParticoes = Math.max(2, Math.ceil(tamanhoMB.toDouble / tamanhoMaximoParticaoMB).toInt)

      println(s"Tamanho dos dados: $tamanhoMB MB")
      println(s"Número de partições calculado: $numParticoes")

      numParticoes
    }

    // Função para processar cada tipo de documento
    tiposDocumentos.foreach { tipoDoc =>
      val origem = s"${baseOrigem}${tipoDoc}/processado/${anoMes}*"
      val destino = s"${baseDestino}${tipoDoc}/${anoMes}"
      println(s"Processando $tipoDoc: Origem: $origem -> Destino: $destino")

      // Leitura dos arquivos Parquet
      val df = spark.read.parquet(origem)

      // Imprime a quantidade de linhas na base de origem
      val quantidadeLinhas = df.count()
      println(s"Quantidade de linhas para $tipoDoc: $quantidadeLinhas")

      // Calcula o número de partições com base no tamanho dos dados
      val numParticoes = calcularNumParticoes(df)

      // Reparticiona e salva no destino com compactação lz4 e modo overwrite
      df.repartition(numParticoes)
        .write
        .mode("overwrite")
        .option("compression", "lz4")
        .parquet(destino)

      println(s"Processamento de $tipoDoc concluído e salvo em $destino")
    }
  }
}
//AgrupamentoParquetPorMes.main(Array())