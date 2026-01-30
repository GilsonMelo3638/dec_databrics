package Utils.Auditoria

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, substring, lit, concat}
import org.apache.hadoop.fs.{FileSystem, Path}

object AgrupamentoParquetPorDia {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Agrupamento Parquet por Dia")
      .getOrCreate()

    // Definir os tipos de documentos e seus diretórios
    val tiposDocumentos = Seq("bpe", "cte", "mdfe", "nf3e", "nfcom",
      "nfe_cancelamento", "nfce_cancelamento", "cte_cancelamento",
      "nf3e_cancelamento", "bpe_cancelamento", "mdfe_cancelamento",
      "nfcom_cancelamento", "nfe_evento", "bpe_evento", "mdfe_evento", "nf3e_evento", "nfcom_evento",
      "nfe", "nfce")

    // Diretório base de origem e destino
    val baseOrigem = "/datalake/bronze/sources/dbms/dec/processamento/"
    val baseDestino = "/datalake/bronze/sources/dbms/dec/diario/"

    // Tamanho máximo por partição (300 MB)
    val tamanhoMaximoParticaoMB = 300

    // Obter o FileSystem do contexto do Spark
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Função para verificar se uma partição já existe
    def particaoExiste(tipoDoc: String, year: String, month: String, day: String): Boolean = {
      val path = new Path(s"${baseDestino}${tipoDoc}/year=${year}/month=${month}/day=${day}")
      fs.exists(path)
    }

    // Função para calcular o número de partições com base no tamanho dos dados
    def calcularNumParticoes(df: DataFrame): Int = {
      val tamanhoBytes = df.queryExecution.optimizedPlan.stats.sizeInBytes.toLong
      val tamanhoMB = tamanhoBytes / (1024 * 1024)
      Math.max(2, Math.ceil(tamanhoMB.toDouble / tamanhoMaximoParticaoMB).toInt)
    }

    // Função para processar cada tipo de documento
    tiposDocumentos.foreach { tipoDoc =>
      val origem = s"${baseOrigem}${tipoDoc}/processado/*"
      val destino = s"${baseDestino}${tipoDoc}/"
      println(s"Processando $tipoDoc: Origem: $origem -> Destino: $destino")

      try {
        // Leitura dos arquivos Parquet
        val df = spark.read.parquet(origem)

        // Adiciona colunas de partição (year, month, day) baseadas em DHPROC
        val partitionedDF = df.withColumn("year", substring(col("DHPROC"), 7, 4))
          .withColumn("month", substring(col("DHPROC"), 4, 2))
          .withColumn("day", substring(col("DHPROC"), 1, 2))
          .withColumn("particao_completa",
            concat(col("year"), lit("/"), col("month"), lit("/"), col("day")))

        // Obter lista de partições únicas no DataFrame
        val particoesNoDF = partitionedDF.select("year", "month", "day").distinct().collect()

        // Filtrar apenas partições que não existem no destino
        val particoesParaProcessar = particoesNoDF.filter { row =>
          val year = row.getString(0)
          val month = row.getString(1)
          val day = row.getString(2)
          !particaoExiste(tipoDoc, year, month, day)
        }

        if (particoesParaProcessar.isEmpty) {
          println(s"Nenhuma partição nova para processar em $tipoDoc")
        } else {
          println(s"Partições a serem processadas para $tipoDoc:")
          particoesParaProcessar.foreach { row =>
            println(s"year=${row.getString(0)}/month=${row.getString(1)}/day=${row.getString(2)}")
          }

          // Criar condição de filtro para manter apenas as partições que não existem
          val condicaoFiltro = particoesParaProcessar.map { row =>
            val year = row.getString(0)
            val month = row.getString(1)
            val day = row.getString(2)
            col("year") === year && col("month") === month && col("day") === day
          }.reduce(_ || _)

          val dfFiltrado = partitionedDF.filter(condicaoFiltro)

          // Imprime a quantidade de linhas a serem processadas
          val quantidadeLinhas = dfFiltrado.count()
          println(s"Quantidade de linhas para $tipoDoc: $quantidadeLinhas")

          if (quantidadeLinhas > 0) {
            // Calcula o número de partições com base no tamanho dos dados
            val numParticoes = calcularNumParticoes(dfFiltrado)

            // Reparticiona e salva no destino com partições por dia, compactação lz4
            dfFiltrado.repartition(numParticoes)
              .write
              .mode("append")  // Usamos append pois já filtramos apenas partições novas
              .option("compression", "lz4")
              .partitionBy("year", "month", "day")
              .parquet(destino)
          }
        }

        println(s"Processamento de $tipoDoc concluído e salvo em $destino")
      } catch {
        case e: Exception =>
          println(s"Erro ao processar $tipoDoc: ${e.getMessage}")
          e.printStackTrace()
      }
    }
  }
}
//AgrupamentoParquetPorDia.main(Array())