package Utils.Auditoria.AuditoriaLogger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}

object AuditoriaDet {

  // Caminhos base
  private val HdfsPathPrata = "/datalake/prata/sources/dbms/dec/"
  private val HdfsPathBronze = "/datalake/bronze/sources/dbms/dec/diario/"
  private val HdfsPathBronzeProcessamento = "/datalake/bronze/sources/dbms/dec/processamento/"

  /** * Configuração Spark */
  def setup(spark: SparkSession): Unit = {
    spark.conf.set(
      "spark.sql.parquet.compression.codec",
      "lz4"
    )
  }

  /**
   * PASSO 1
   * Identificar chaves faltantes no prata
   */
  def identificarChavesFaltantesNoPrata(
                                         spark: SparkSession,
                                         documentType: String,
                                         anoInicio: Int,
                                         mesInicio: Int,
                                         anoFim: Int,
                                         mesFim: Int,
                                         logFn: String => Unit = println
                                       ): Unit = {

    logFn(
      s"""
         |==================================================
         |PASSO 1 - Identificar chaves faltantes no prata
         |Documento: $documentType
         |Período : $anoInicio/$mesInicio até $anoFim/$mesFim
         |==================================================
         |""".stripMargin
    )

    try {

      // Caminhos
      val (
        bronzeBasePath,
        prataPath,
        faltantesBasePath,
        _,
        _,
        _,
        _
        ) = getPaths(documentType)

      logFn(s"Lendo camada prata: $prataPath")

      val prataDF = spark.read
        .parquet(prataPath)
        .select(
          lower(col("chave")).alias("chave")
        )
        .distinct()

      val totalPrata = prataDF.count()

      logFn(
        s"Quantidade de chaves distintas na prata: $totalPrata"
      )

      for (ano <- anoInicio to anoFim) {

        for (
          mes <- mesInicio to 12
          if !(ano == anoFim && mes > mesFim)
        ) {

          val formattedMonth = f"$mes%02d"

          val bronzePath =
            s"${bronzeBasePath}year=$ano/month=$formattedMonth"

          val faltantesPath =
            s"${faltantesBasePath}year=$ano/month=$formattedMonth"

          logFn(
            s"""
               |--------------------------------------------------
               |Processando:
               |Ano : $ano
               |Mês : $formattedMonth
               |Bronze: $bronzePath
               |Destino faltantes: $faltantesPath
               |--------------------------------------------------
               |""".stripMargin
          )

          val fs = FileSystem.get(
            spark.sparkContext.hadoopConfiguration
          )

          if (fs.exists(new Path(bronzePath))) {

            logFn(
              s"Caminho bronze encontrado: $bronzePath"
            )

            val bronzeDF = spark.read
              .parquet(bronzePath)
              .withColumn(
                "chave",
                lower(col("CHAVE"))
              )

            val totalBronze = bronzeDF.count()

            logFn(
              s"Quantidade registros bronze: $totalBronze"
            )

            val chavesNaoExistentes =
              bronzeDF.join(
                prataDF,
                Seq("chave"),
                "left_anti"
              )

            val totalFaltantes =
              chavesNaoExistentes.count()

            logFn(
              s"Quantidade de chaves faltantes encontradas: $totalFaltantes"
            )

            if (totalFaltantes > 0) {

              logFn(
                s"Iniciando gravação das chaves faltantes..."
              )

              chavesNaoExistentes
                .repartition(10)
                .write
                .format("parquet")
                .mode("overwrite")
                .option("compression", "lz4")
                .save(faltantesPath)

              logFn(
                s"""
                   |✅ Chaves faltantes salvas com sucesso
                   |Destino: $faltantesPath
                   |Quantidade: $totalFaltantes
                   |""".stripMargin
              )

            } else {

              logFn(
                s"""
                   |Nenhuma chave faltante encontrada
                   |Ano : $ano
                   |Mês : $formattedMonth
                   |""".stripMargin
              )
            }

          } else {

            logFn(
              s"""
                 |⚠️ Caminho bronze inexistente
                 |Caminho: $bronzePath
                 |""".stripMargin
            )
          }
        }
      }

      logFn(
        s"""
           |PASSO 1 concluído com sucesso
           |Documento: $documentType
           |""".stripMargin
      )

    } catch {

      case e: Exception =>

        logFn(
          s"""
             |❌ ERRO no PASSO 1
             |Documento: $documentType
             |Mensagem: ${e.getMessage}
             |StackTrace:
             |${e.getStackTrace.mkString("\n")}
             |""".stripMargin
        )

        throw e
    }
  }

  /**
   * PASSO 2
   * Identificar ausências em DET
   */
  def identificarAusencias(
                            spark: SparkSession,
                            documentType: String,
                            logFn: String => Unit = println
                          ): Unit = {

    logFn(
      s"""
         |==================================================
         |PASSO 2 - Identificar ausências DET
         |Documento: $documentType
         |==================================================
         |""".stripMargin
    )

    try {

      val (
        _,
        _,
        _,
        pathInf,
        pathDet,
        savePathChavesFaltantes,
        _
        ) = getPaths(documentType)

      logFn(s"Lendo INF: $pathInf")

      val dfInf =
        spark.read.parquet(pathInf)

      logFn(s"Lendo DET: $pathDet")

      val dfDet =
        spark.read.parquet(pathDet)

      val distinctInf =
        dfInf.select("chave").distinct()

      val distinctDet =
        dfDet.select("chave").distinct()

      val totalInf = distinctInf.count()
      val totalDet = distinctDet.count()

      logFn(
        s"""
           |Quantidade chaves INF: $totalInf
           |Quantidade chaves DET: $totalDet
           |""".stripMargin
      )

      val diffChaves =
        distinctInf.except(distinctDet)

      val totalDiff =
        diffChaves.count()

      if (totalDiff > 0) {

        logFn(
          s"""
             |Quantidade de chaves faltantes no DET: $totalDiff
             |Destino: $savePathChavesFaltantes
             |""".stripMargin
        )

        diffChaves.show(false)

        diffChaves.write
          .mode("overwrite")
          .option("compression", "lz4")
          .parquet(savePathChavesFaltantes)

        logFn(
          s"""
             |✅ Resultado salvo com sucesso
             |Destino: $savePathChavesFaltantes
             |""".stripMargin
        )

      } else {

        logFn(
          s"""
             |Nenhuma chave faltante encontrada
             |Documento: $documentType
             |""".stripMargin
        )
      }

      logFn(
        s"""
           |PASSO 2 concluído com sucesso
           |Documento: $documentType
           |""".stripMargin
      )

    } catch {

      case e: Exception =>

        logFn(
          s"""
             |❌ ERRO no PASSO 2
             |Documento: $documentType
             |Mensagem: ${e.getMessage}
             |StackTrace:
             |${e.getStackTrace.mkString("\n")}
             |""".stripMargin
        )

        throw e
    }
  }

  /**
   * PASSO 3
   * Verificar duplicidade DET
   */
  def verificarDuplicidade(
                            spark: SparkSession,
                            documentType: String,
                            logFn: String => Unit = println
                          ): Unit = {

    logFn(
      s"""
         |==================================================
         |PASSO 3 - Verificar duplicidade DET
         |Documento: $documentType
         |==================================================
         |""".stripMargin
    )

    try {

      val (
        _,
        _,
        _,
        _,
        pathDet,
        _,
        duplicatesPath
        ) = getPaths(documentType)

      logFn(s"Lendo DET: $pathDet")

      val df = spark.read
        .option("basePath", pathDet)
        .parquet(pathDet)

      val totalRegistros = df.count()

      logFn(
        s"Quantidade registros DET: $totalRegistros"
      )

      val duplicatesByKey = df
        .groupBy("CHAVE", "nitem")
        .count()
        .filter("count > 1")

      val totalDuplicidades =
        duplicatesByKey.count()

      if (totalDuplicidades == 0) {

        logFn(
          s"""
             |Nenhuma duplicidade encontrada
             |Documento: $documentType
             |""".stripMargin
        )

        return
      }

      logFn(
        s"""
           |Duplicidades encontradas: $totalDuplicidades
           |Destino: $duplicatesPath
           |""".stripMargin
      )

      duplicatesByKey.show(false)

      val duplicatesRecords =
        df.join(
          duplicatesByKey,
          Seq("CHAVE", "nitem")
        )

      val totalRegistrosDuplicados =
        duplicatesRecords.count()

      logFn(
        s"""
           |Quantidade registros duplicados: $totalRegistrosDuplicados
           |""".stripMargin
      )

      duplicatesRecords.write
        .mode("overwrite")
        .option("compression", "lz4")
        .parquet(duplicatesPath)

      logFn(
        s"""
           |✅ Duplicidades salvas com sucesso
           |Destino: $duplicatesPath
           |""".stripMargin
      )

      logFn(
        s"""
           |PASSO 3 concluído com sucesso
           |Documento: $documentType
           |""".stripMargin
      )

    } catch {

      case e: Exception =>

        logFn(
          s"""
             |❌ ERRO no PASSO 3
             |Documento: $documentType
             |Mensagem: ${e.getMessage}
             |StackTrace:
             |${e.getStackTrace.mkString("\n")}
             |""".stripMargin
        )

        throw e
    }
  }

  /**
   * Caminhos por tipo documento
   */
  private def getPaths(
                        documentType: String
                      ): (
    String,
      String,
      String,
      String,
      String,
      String,
      String
    ) = {

    documentType.toLowerCase match {

      case "nfe" =>

        (
          s"${HdfsPathBronze}nfe/",
          s"${HdfsPathPrata}nfe/infNFe/",
          s"${HdfsPathBronzeProcessamento}nfe/faltantes/",
          s"${HdfsPathPrata}nfe/infNFe/",
          s"${HdfsPathPrata}nfe/det/",
          s"${HdfsPathBronzeProcessamento}nfe/chaves_faltantes/",
          s"${HdfsPathBronzeProcessamento}nfe/det_duplicados"
        )

      case "nfce" =>

        (
          s"${HdfsPathBronze}nfce/",
          s"${HdfsPathPrata}nfce/infNFCe/",
          s"${HdfsPathBronzeProcessamento}nfce/faltantes/",
          s"${HdfsPathPrata}nfce/infNFCe/",
          s"${HdfsPathPrata}nfce/det/",
          s"${HdfsPathBronzeProcessamento}nfce/chaves_faltantes/",
          s"${HdfsPathBronzeProcessamento}nfce/det_duplicados"
        )

      case _ =>

        throw new IllegalArgumentException(
          s"""
             |Tipo de documento não suportado: $documentType
             |Use apenas: nfe ou nfce
             |""".stripMargin
        )
    }
  }
}