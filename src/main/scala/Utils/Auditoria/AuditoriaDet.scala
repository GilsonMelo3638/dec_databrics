package Auditoria

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}

object AuditoriaDet {
  // Caminhos base
  private val HdfsPathPrata = "/datalake/prata/sources/dbms/dec/"
  private val HdfsPathBronze = "/datalake/bronze/sources/dbms/dec/diario/"
  private val HdfsPathBronzeProcessamento = "/datalake/bronze/sources/dbms/dec/processamento/"

  // Configurar compactação LZ4 (pode ser feito uma vez no início)
  def setup(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")
  }

  /**
   * PASSO 1: Identificar chaves faltantes no Prata
   */
  def identificarChavesFaltantesNoPrata(spark: SparkSession, documentType: String, anoInicio: Int, mesInicio: Int, anoFim: Int, mesFim: Int): Unit = {
    println("Iniciando PASSO 1: Identificar chaves faltantes no Prata...")

    // Obter caminhos com base no tipo de documento
    val (bronzeBasePath, prataPath, faltantesBasePath, _, _, _, _) = getPaths(documentType)

    val prataDF = spark.read.parquet(prataPath).select(lower(col("chave")).alias("chave")).distinct()

    for (ano <- anoInicio to anoFim) {
      for (mes <- mesInicio to 12 if !(ano == anoFim && mes > mesFim)) {
        val formattedMonth = f"$mes%02d"
        val bronzePath = s"${bronzeBasePath}year=$ano/month=$formattedMonth"
        val faltantesPath = s"${faltantesBasePath}year=$ano/month=$formattedMonth"

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

            println(s"Chaves do bronze que não existem no prata para year=$ano/month=$formattedMonth foram salvas em: $faltantesPath")
          } else println(s"Nenhuma chave faltante para year=$ano/month=$formattedMonth.")
        } else println(s"O caminho $bronzePath não existe.")
      }
    }
    println("Processamento do PASSO 1 concluído!")
  }

  /**
   * PASSO 2: Identificar ausências em DET com base em INFNFE
   */
  def identificarAusencias(spark: SparkSession, documentType: String): Unit = {
    println("Iniciando PASSO 2: Identificar ausências em DET com base em INFNFE...")

    val (_, _, _, pathInf, pathDet, savePathChavesFaltantes, _) = getPaths(documentType)

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
    } else println("Nenhuma chave faltante encontrada. Nada a ser exibido ou salvo.")
    println("Processamento do PASSO 2 concluído!")
  }

  /**
   * PASSO 3: Verificar se há duplicidade em DET
   */
  def verificarDuplicidade(spark: SparkSession, documentType: String): Unit = {
    println("Iniciando PASSO 3: Verificar se há duplicidade em DET...")

    val (_, _, _, _, pathDet, _, duplicatesPath) = getPaths(documentType)

    val df = spark.read.option("basePath", pathDet).parquet(pathDet)

    val duplicatesByKey = df.groupBy("CHAVE", "nitem").count().filter("count > 1")
    duplicatesByKey.show()

    if (duplicatesByKey.isEmpty) {
      println("Nenhuma duplicidade encontrada. Encerrando processo.")
      return
    }

    val duplicatesRecords = df.join(duplicatesByKey, Seq("CHAVE", "nitem"))
    duplicatesRecords.write.mode("overwrite").parquet(duplicatesPath)
    println(s"Duplicidades salvas em: $duplicatesPath")

    println("Processamento do PASSO 3 concluído!")
  }

  private def getPaths(documentType: String): (String, String, String, String, String, String, String) = {
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
}