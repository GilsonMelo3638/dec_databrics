package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.hadoop.fs.{FileSystem, Path}

class AuditoriaDet(spark: SparkSession, documentType: String) {
  // Define os caminhos base com base no tipo de documento
  private val HdfsPathPrata = "/datalake/prata/sources/dbms/dec/"
  private val HdfsPathBronze = "/datalake/bronze/sources/dbms/dec/"
  private val HdfsPathBronzeProcessamento = "/datalake/bronze/sources/dbms/dec/processamento/"

  // Configurações base (definidas dinamicamente com base no tipo de documento)
  private val (bronzeBasePath, prataPath, faltantesBasePath, pathInf, pathDet, savePathChavesFaltantes, duplicatesPath) = {
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
        throw new IllegalArgumentException(s"Tipo de documento não suportado: $documentType. Use 'nfe' ou 'nfce'.")
    }
  }

  // Configurar compactação LZ4
  spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

  /**
   * PASSO 1: Gerar dados duplicados
   */
  def gerarDadosDuplicados(anoInicio: Int, mesInicio: Int, anoFim: Int, mesFim: Int): Unit = {
    val prataDF = spark.read.parquet(prataPath).select(lower(col("chave")).alias("chave")).distinct()

    for (ano <- anoInicio to anoFim) {
      for (mes <- mesInicio to 12 if !(ano == anoFim && mes > mesFim)) {
        val anoMes = f"$ano${mes}%02d"
        val bronzePath = s"${bronzeBasePath}${anoMes}"
        val faltantesPath = s"${faltantesBasePath}${anoMes}"

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (fs.exists(new Path(bronzePath))) {
          val bronzeDF = spark.read.parquet(bronzePath).withColumn("chave", lower(col("CHAVE")))
          val chavesNaoExistentes = bronzeDF.join(prataDF, Seq("chave"), "left_anti")

          if (!chavesNaoExistentes.isEmpty) {
            chavesNaoExistentes.repartition(10)
              .write.format("parquet")
              .mode("overwrite")
              .option("compression", "lz4")
              .save(faltantesPath)

            println(s"Chaves do bronze que não existem no prata para $anoMes foram salvas em: $faltantesPath")
          } else {
            println(s"Nenhuma chave faltante para $anoMes.")
          }
        } else {
          println(s"O caminho $bronzePath não existe.")
        }
      }
    }
    println("Processamento concluído!")
  }

  /**
   * PASSO 2: Identificar ausências em DET com base em INFNFE
   */
  def identificarAusencias(): Unit = {
    val dfInf = spark.read.parquet(pathInf)
    val dfDet = spark.read.parquet(pathDet)

    val distinctInf = dfInf.select("chave").distinct()
    val distinctDet = dfDet.select("chave").distinct()

    val diffChaves = distinctInf.except(distinctDet)

    if (!diffChaves.isEmpty) {
      diffChaves.show(false)
      val countDiffChaves = diffChaves.count()
      println(s"Total de 'chave' distintas que existem em 'inf${documentType.toUpperCase}' mas não em 'det': $countDiffChaves")
      diffChaves.write.mode("overwrite").parquet(savePathChavesFaltantes)
      println(s"Resultado salvo em: $savePathChavesFaltantes")
    } else {
      println("Nenhuma chave faltante encontrada. Nada a ser exibido ou salvo.")
    }
  }

  /**
   * PASSO 3: Verificar se há duplicidade em DET
   */
  def verificarDuplicidade(): Unit = {
    val df = spark.read.option("basePath", pathDet).parquet(pathDet)
    // df.printSchema()

    val duplicatesByKey = df.groupBy("CHAVE", "nitem").count().filter("count > 1")
    duplicatesByKey.show()

    val duplicatesRecords = df.join(duplicatesByKey, Seq("CHAVE", "nitem"))
    duplicatesRecords.write.mode("overwrite").parquet(duplicatesPath)
    println(s"Duplicidades salvas em: $duplicatesPath")
  }
}

// Exemplo de uso:
val spark = SparkSession.builder.appName("AuditoriaDet").getOrCreate()

// Para NFE
val processorNFe = new AuditoriaDet(spark, "nfe")
processorNFe.gerarDadosDuplicados(2025, 1, 2025, 3)
processorNFe.identificarAusencias()
processorNFe.verificarDuplicidade()

// Para NFCE
val processorNFCe = new AuditoriaDet(spark, "nfce")
processorNFCe.gerarDadosDuplicados(2025, 1, 2025, 3)
processorNFCe.identificarAusencias()
processorNFCe.verificarDuplicidade()