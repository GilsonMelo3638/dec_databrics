package Utils.Auditoria

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, substring}

object AgrupamentoParquetPorDia {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Agrupamento Parquet por Dia")
      .getOrCreate()

    // Definir os tipos de documentos e seus diretórios
    val tiposDocumentos = Seq("bpe", "cte", "mdfe", "nf3e", "nfe_cancelamento", "nfce_cancelamento", "cte_cancelamento", "nf3e_cancelamento", "bpe_cancelamento", "mdfe_cancelamento", "nfe", "nfce")

    // Diretório base de origem e destino
    val baseOrigem = "/datalake/bronze/sources/dbms/dec/processamento/"
    val baseDestino = "/datalake/bronze/sources/dbms/dec/diario/"

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
      val origem = s"${baseOrigem}${tipoDoc}/processado/*"
      val destino = s"${baseDestino}${tipoDoc}/"
      println(s"Processando $tipoDoc: Origem: $origem -> Destino: $destino")

      // Leitura dos arquivos Parquet
      val df = spark.read.parquet(origem)

      // Adiciona colunas de partição (year, month, day) baseadas em DHPROC
      val partitionedDF = df.withColumn("year", substring(col("DHPROC"), 7, 4))
        .withColumn("month", substring(col("DHPROC"), 4, 2))
        .withColumn("day", substring(col("DHPROC"), 1, 2))

      // Imprime a quantidade de linhas na base de origem
      val quantidadeLinhas = partitionedDF.count()
      println(s"Quantidade de linhas para $tipoDoc: $quantidadeLinhas")

      // Calcula o número de partições com base no tamanho dos dados
      val numParticoes = calcularNumParticoes(partitionedDF)

      // Reparticiona e salva no destino com partições por dia, compactação lz4 e pulando se existir
      partitionedDF.repartition(numParticoes)
        .write
        .mode("ignore")  // Pula se a partição já existir
        .option("compression", "lz4")
        .partitionBy("year", "month", "day")
        .parquet(destino)

      println(s"Processamento de $tipoDoc concluído e salvo em $destino")
    }
  }
}
//AgrupamentoParquetPorDia.main(Array())