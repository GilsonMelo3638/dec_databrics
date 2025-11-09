package Auditoria

import Utils.Auditoria.AgrupamentoParquetPorDia
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.hadoop.fs.{FileSystem, Path}

object Auditoria {
  def main(args: Array[String]): Unit = {
    // Default values if not provided in args
    var year = "2025"
    var month = "10"
    var anoInicio = 2025
    var mesInicio = 10
    var anoFim = 2025
    var mesFim = 10

    // Parse arguments if provided
    if (args.length >= 6) {
      year = args(0)
      month = args(1)
      anoInicio = args(2).toInt
      mesInicio = args(3).toInt
      anoFim = args(4).toInt
      mesFim = args(5).toInt
    }

    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("AuditoriaDocumentosFiscais")
      .getOrCreate()

    try {
      // Executa os processos prévios
      UltimaPastaHDFS.main(Array())
      AgrupamentoParquetPorDia.main(Array())

      // Processa os documentos
      processarTodosDocumentos(spark, year, month)

      // Processa NFE e NFCE
      processarNFeNFCE(spark, anoInicio, mesInicio, anoFim, mesFim)

    } finally {
      spark.close()
    }
  }

  private def processarTodosDocumentos(spark: SparkSession, year: String, month: String): Unit = {
    // Lista de configurações para cada tipo de documento
    val documentosConfig = List(
      ("/datalake/bronze/sources/dbms/dec/diario/bpe/", "/datalake/prata/sources/dbms/dec/bpe/BPe/", None, None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/mdfe/", "/datalake/prata/sources/dbms/dec/mdfe/MDFe/", None, None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/cte/", "/datalake/prata/sources/dbms/dec/cte/CTeSimp/", Some(57), Some("<cteSimpProc"), "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/cte/", "/datalake/prata/sources/dbms/dec/cte/CTe/", Some(57), Some("<cteProc"), "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/cte/", "/datalake/prata/sources/dbms/dec/cte/CTeOS/", Some(67), None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/cte/", "/datalake/prata/sources/dbms/dec/cte/GVTe/", Some(64), None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/nf3e/", "/datalake/prata/sources/dbms/dec/nf3e/NF3e/", None, None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfcom/", "/datalake/prata/sources/dbms/dec/nfcom/NFCom/", None, None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/bpe_cancelamento/", "/datalake/prata/sources/dbms/dec/bpe/cancelamento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/nf3e_cancelamento/", "/datalake/prata/sources/dbms/dec/nf3e/cancelamento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfce_cancelamento/", "/datalake/prata/sources/dbms/dec/nfce/cancelamento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/mdfe_cancelamento/", "/datalake/prata/sources/dbms/dec/mdfe/cancelamento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfcom_cancelamento/", "/datalake/prata/sources/dbms/dec/nfcom/cancelamento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfe_cancelamento/", "/datalake/prata/sources/dbms/dec/nfe/cancelamento/", None, None, "NSUDF"),
      ("/datalake/bronze/sources/dbms/dec/diario/cte_cancelamento/", "/datalake/prata/sources/dbms/dec/cte/cancelamento/", None, None, "NSUSVD"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfe/", "/datalake/prata/sources/dbms/dec/nfe/infNFe/", None, None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfce/", "/datalake/prata/sources/dbms/dec/nfce/infNFCe/", None, None, "CHAVE"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfe_evento/", "/datalake/prata/sources/dbms/dec/nfe/evento/", None, None, "NSUDF"),
      ("/datalake/bronze/sources/dbms/dec/diario/mdfe_evento/", "/datalake/prata/sources/dbms/dec/mdfe/evento/", None, None, "NSU")
    )

    // Processa cada documento
    documentosConfig.foreach { case (bronzePath, prataPath, modelo, filtroXML, colunaVerificacao) =>
      colunaVerificacao match {
        case "NSUDF" => processarDocumentoComNSUDF(spark, bronzePath, prataPath, year, month, modelo, filtroXML)
        case "NSU" => processarDocumentoComNSU(spark, bronzePath, prataPath, year, month, modelo, filtroXML)
        case "NSUSVD" => processarDocumentoComNSUSVD(spark, bronzePath, prataPath, year, month, modelo, filtroXML)
        case _ => processarDocumento(spark, bronzePath, prataPath, year, month, modelo, filtroXML, colunaVerificacao)
      }
    }
  }

  private def processarDocumento(
                                  spark: SparkSession,
                                  bronzeBasePath: String,
                                  prataPath: String,
                                  year: String,
                                  month: String,
                                  modelo: Option[Int] = None,
                                  filtroXML: Option[String] = None,
                                  colunaVerificacao: String = "CHAVE"
                                ): Unit = {
    println(s"\nProcessando documento: $prataPath")

    val prataDF = spark.read.parquet(prataPath).select(colunaVerificacao.toLowerCase)
    val chavesRepetidasPrata = prataDF.groupBy(colunaVerificacao.toLowerCase).count().filter(col("count") > 1)

    println(s"${colunaVerificacao}s repetidas (quantidade > 1) no prata:")
    chavesRepetidasPrata.show(10, false)

    val bronzePath = s"${bronzeBasePath}year=${year}/month=${month}"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathExists = fs.exists(new Path(bronzePath))

    if (pathExists) {
      var bronzeDF = spark.read.parquet(bronzePath)
      if (modelo.isDefined) {
        bronzeDF = bronzeDF.filter(col("MODELO") === modelo.get)
      }
      if (filtroXML.isDefined) {
        bronzeDF = bronzeDF.filter(col("XML_DOCUMENTO_CLOB").rlike(filtroXML.get))
      }
      bronzeDF = bronzeDF.select(col(colunaVerificacao).alias(colunaVerificacao.toLowerCase)).distinct()

      val chavesNaoExistentes = bronzeDF.except(prataDF)
      println(s"${colunaVerificacao}s do bronze que não existem no prata para year=${year}/month=${month}:")
      chavesNaoExistentes.show(100, false)
    } else {
      println(s"O caminho $bronzePath não existe.")
    }
  }

  private def processarDocumentoComNSUDF(
                                          spark: SparkSession,
                                          bronzeBasePath: String,
                                          prataPath: String,
                                          year: String,
                                          month: String,
                                          modelo: Option[Int] = None,
                                          filtroXML: Option[String] = None
                                        ): Unit = {
    println(s"\nProcessando documento com NSUDF: $prataPath")

    val prataDF = spark.read.parquet(prataPath).select("nsudf")
    val nsudfsRepetidosPrata = prataDF.groupBy("nsudf").count().filter(col("count") > 1)

    println(s"NSUDFs repetidos (quantidade > 1) no prata:")
    nsudfsRepetidosPrata.show(10, false)

    val bronzePath = s"${bronzeBasePath}year=${year}/month=${month}"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathExists = fs.exists(new Path(bronzePath))

    if (pathExists) {
      var bronzeDF = spark.read.parquet(bronzePath)
      if (modelo.isDefined) {
        bronzeDF = bronzeDF.filter(col("MODELO") === modelo.get)
      }
      if (filtroXML.isDefined) {
        bronzeDF = bronzeDF.filter(col("XML_DOCUMENTO_CLOB").rlike(filtroXML.get))
      }
      bronzeDF = bronzeDF.select(col("NSUDF").alias("nsudf")).distinct()

      val nsudfsNaoExistentes = bronzeDF.except(prataDF)
      println(s"NSUDFs do bronze que não existem no prata para year=${year}/month=${month}:")
      nsudfsNaoExistentes.show(100, false)
    } else {
      println(s"O caminho $bronzePath não existe.")
    }
  }

  private def processarDocumentoComNSU(
                                        spark: SparkSession,
                                        bronzeBasePath: String,
                                        prataPath: String,
                                        year: String,
                                        month: String,
                                        modelo: Option[Int] = None,
                                        filtroXML: Option[String] = None
                                      ): Unit = {
    println(s"\nProcessando documento com NSU: $prataPath")

    val prataDF = spark.read.parquet(prataPath).select("nsu")
    val nsusRepetidosPrata = prataDF.groupBy("nsu").count().filter(col("count") > 1)

    println(s"NSUs repetidos (quantidade > 1) no prata:")
    nsusRepetidosPrata.show(10, false)

    val bronzePath = s"${bronzeBasePath}year=${year}/month=${month}"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathExists = fs.exists(new Path(bronzePath))

    if (pathExists) {
      var bronzeDF = spark.read.parquet(bronzePath)
      if (modelo.isDefined) {
        bronzeDF = bronzeDF.filter(col("MODELO") === modelo.get)
      }
      if (filtroXML.isDefined) {
        bronzeDF = bronzeDF.filter(col("XML_DOCUMENTO_CLOB").rlike(filtroXML.get))
      }
      bronzeDF = bronzeDF.select(col("NSU").alias("nsu")).distinct()

      val nsusNaoExistentes = bronzeDF.except(prataDF)
      println(s"NSUs do bronze que não existem no prata para year=${year}/month=${month}:")
      nsusNaoExistentes.show(100, false)
    } else {
      println(s"O caminho $bronzePath não existe.")
    }
  }

  private def processarDocumentoComNSUSVD(
                                           spark: SparkSession,
                                           bronzeBasePath: String,
                                           prataPath: String,
                                           year: String,
                                           month: String,
                                           modelo: Option[Int] = None,
                                           filtroXML: Option[String] = None
                                         ): Unit = {
    println(s"\nProcessando documento com NSUSVD: $prataPath")

    val prataDF = spark.read.parquet(prataPath).select("nsusvd")
    val nsusvdRepetidosPrata = prataDF.groupBy("nsusvd").count().filter(col("count") > 1)

    println(s"NSUSVDs repetidos (quantidade > 1) no prata:")
    nsusvdRepetidosPrata.show(10, false)

    val bronzePath = s"${bronzeBasePath}year=${year}/month=${month}"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathExists = fs.exists(new Path(bronzePath))

    if (pathExists) {
      var bronzeDF = spark.read.parquet(bronzePath)
      if (modelo.isDefined) {
        bronzeDF = bronzeDF.filter(col("MODELO") === modelo.get)
      }
      if (filtroXML.isDefined) {
        bronzeDF = bronzeDF.filter(col("XML_DOCUMENTO_CLOB").rlike(filtroXML.get))
      }
      bronzeDF = bronzeDF.select(col("NSUSVD").alias("nsusvd")).distinct()

      val nsusvdNaoExistentes = bronzeDF.except(prataDF)
      println(s"NSUSVDs do bronze que não existem no prata para year=${year}/month=${month}:")
      nsusvdNaoExistentes.show(100, false)
    } else {
      println(s"O caminho $bronzePath não existe.")
    }
  }

  private def processarNFeNFCE(spark: SparkSession, anoInicio: Int, mesInicio: Int, anoFim: Int, mesFim: Int): Unit = {
    // Configura a compactação LZ4
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    // Para NFE
    println("\nProcessando NFE:")
    AuditoriaDet.identificarChavesFaltantesNoPrata(spark, "nfe", anoInicio, mesInicio, anoFim, mesFim)
    AuditoriaDet.identificarAusencias(spark, "nfe")
    AuditoriaDet.verificarDuplicidade(spark, "nfe")

    // Para NFCE
    println("\nProcessando NFCE:")
    AuditoriaDet.identificarChavesFaltantesNoPrata(spark, "nfce", anoInicio, mesInicio, anoFim, mesFim)
    AuditoriaDet.identificarAusencias(spark, "nfce")
    AuditoriaDet.verificarDuplicidade(spark, "nfce")
  }
}

//Auditoria.main(Array())