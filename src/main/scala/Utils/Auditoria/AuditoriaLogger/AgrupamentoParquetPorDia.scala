package Utils.Auditoria.AuditoriaLogger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, concat, lit, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AgrupamentoParquetPorDia {

  /**
   * Execução standalone
   */
  def main(args: Array[String]): Unit = {
    executar()
  }

  /**
   * Método reutilizável com suporte a logger customizado.
   *
   * Standalone:
   * AgrupamentoParquetPorDia.executar()
   *
   * Via AuditoriaLogger:
   * AgrupamentoParquetPorDia.executar(HDFSLogger.log)
   */
  def executar(logFn: String => Unit = println): Unit = {

    val spark = SparkSession.builder()
      .appName("Agrupamento Parquet por Dia")
      .getOrCreate()

    try {

      logFn("Iniciando AgrupamentoParquetPorDia")

      // Tipos de documentos
      val tiposDocumentos = Seq(
        "bpe",
        "bpe_cancelamento",
        "bpe_evento",
        "cte",
        "cte_cancelamento",
        "cte_evento",
        "mdfe",
        "mdfe_cancelamento",
        "mdfe_evento",
        "nf3e",
        "nf3e_cancelamento",
        "nf3e_evento",
        "nfce",
        "nfce_cancelamento",
        "nfcom",
        "nfcom_cancelamento",
        "nfcom_evento",
        "nfe",
        "nfe_cancelamento",
        "nfe_evento"
      )

      // Diretórios
      val baseOrigem = "/datalake/bronze/sources/dbms/dec/processamento/"
      val baseDestino = "/datalake/bronze/sources/dbms/dec/diario/"

      // Tamanho máximo partição
      val tamanhoMaximoParticaoMB = 300

      // FileSystem
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      /**
       * Verifica existência da partição
       */
      def particaoExiste(
                          tipoDoc: String,
                          year: String,
                          month: String,
                          day: String
                        ): Boolean = {

        val path = new Path(
          s"${baseDestino}${tipoDoc}/year=${year}/month=${month}/day=${day}"
        )

        fs.exists(path)
      }

      /**
       * Calcula número de partições
       */
      def calcularNumParticoes(df: DataFrame): Int = {

        val tamanhoBytes =
          df.queryExecution.optimizedPlan.stats.sizeInBytes.toLong

        val tamanhoMB =
          tamanhoBytes / (1024 * 1024)

        Math.max(
          2,
          Math.ceil(
            tamanhoMB.toDouble / tamanhoMaximoParticaoMB
          ).toInt
        )
      }

      logFn(
        s"Quantidade de tipos de documentos a processar: ${tiposDocumentos.size}"
      )

      // Processa cada tipo
      tiposDocumentos.foreach { tipoDoc =>

        val origem =
          s"${baseOrigem}${tipoDoc}/processado/*"

        val destino =
          s"${baseDestino}${tipoDoc}/"

        logFn(
          s"""
             |==================================================
             |Processando tipo de documento: $tipoDoc
             |Origem : $origem
             |Destino: $destino
             |==================================================
             |""".stripMargin
        )

        try {

          // Leitura parquet
          val df = spark.read.parquet(origem)

          logFn(s"Leitura parquet concluída para $tipoDoc")

          // Criação colunas partição
          val partitionedDF = df
            .withColumn("year", substring(col("DHPROC"), 7, 4))
            .withColumn("month", substring(col("DHPROC"), 4, 2))
            .withColumn("day", substring(col("DHPROC"), 1, 2))
            .withColumn(
              "particao_completa",
              concat(
                col("year"),
                lit("/"),
                col("month"),
                lit("/"),
                col("day")
              )
            )

          // Partições únicas
          val particoesNoDF =
            partitionedDF
              .select("year", "month", "day")
              .distinct()
              .collect()

          logFn(
            s"Quantidade de partições encontradas no DataFrame: ${particoesNoDF.length}"
          )

          // Apenas partições inexistentes
          val particoesParaProcessar = particoesNoDF.filter { row =>

            val year = row.getString(0)
            val month = row.getString(1)
            val day = row.getString(2)

            !particaoExiste(tipoDoc, year, month, day)
          }

          if (particoesParaProcessar.isEmpty) {

            logFn(
              s"Nenhuma partição nova encontrada para processamento em $tipoDoc"
            )

          } else {

            logFn(
              s"""
                 |Partições novas encontradas para $tipoDoc:
                 |Quantidade: ${particoesParaProcessar.length}
                 |""".stripMargin
            )

            particoesParaProcessar.foreach { row =>

              val year = row.getString(0)
              val month = row.getString(1)
              val day = row.getString(2)

              logFn(
                s"Nova partição: year=$year/month=$month/day=$day"
              )
            }

            // Filtro partições novas
            val condicaoFiltro = particoesParaProcessar
              .map { row =>

                val year = row.getString(0)
                val month = row.getString(1)
                val day = row.getString(2)

                col("year") === year &&
                  col("month") === month &&
                  col("day") === day

              }
              .reduce(_ || _)

            val dfFiltrado =
              partitionedDF.filter(condicaoFiltro)

            // Contagem linhas
            val quantidadeLinhas =
              dfFiltrado.count()

            logFn(
              s"Quantidade de linhas a processar para $tipoDoc: $quantidadeLinhas"
            )

            if (quantidadeLinhas > 0) {

              // Número partições
              val numParticoes =
                calcularNumParticoes(dfFiltrado)

              logFn(
                s"Número de partições calculadas para escrita: $numParticoes"
              )

              logFn(
                s"Iniciando gravação parquet para $tipoDoc"
              )

              // Escrita parquet
              dfFiltrado
                .repartition(numParticoes)
                .write
                .mode("append")
                .option("compression", "lz4")
                .partitionBy("year", "month", "day")
                .parquet(destino)

              logFn(
                s"""
                   |Gravação concluída com sucesso:
                   |Tipo documento: $tipoDoc
                   |Destino: $destino
                   |Linhas gravadas: $quantidadeLinhas
                   |""".stripMargin
              )

            } else {

              logFn(
                s"""
                   |⚠️ Nenhuma linha encontrada após filtro:
                   |Tipo documento: $tipoDoc
                   |""".stripMargin
              )
            }
          }

          logFn(
            s"Processamento finalizado para tipo documento: $tipoDoc"
          )

        } catch {

          case e: Exception =>

            logFn(
              s"""
                 |❌ ERRO ao processar tipo documento
                 |Tipo documento: $tipoDoc
                 |Mensagem erro: ${e.getMessage}
                 |StackTrace:
                 |${e.getStackTrace.mkString("\n")}
                 |""".stripMargin
            )
        }
      }

      logFn("AgrupamentoParquetPorDia finalizado com sucesso")

    } catch {

      case e: Exception =>

        logFn(
          s"""
             |❌ ERRO FATAL no AgrupamentoParquetPorDia
             |Mensagem erro: ${e.getMessage}
             |StackTrace:
             |${e.getStackTrace.mkString("\n")}
             |""".stripMargin
        )

        throw e

    } finally {

      logFn("SparkSession encerrada no AgrupamentoParquetPorDia")
    }
  }
}

// Execução standalone
// AgrupamentoParquetPorDia.main(Array())