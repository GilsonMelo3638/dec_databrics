package Utils.Auditoria

import Auditoria.{AuditoriaDet, UltimaPastaHDFS}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.text.SimpleDateFormat
import java.util.Date

// ========== HDFSLogger ==========
object HDFSLogger {
  private var outputStream: Option[FSDataOutputStream] = None
  private var logPath: Option[String] = None

  def initialize(spark: SparkSession): Unit = {
    try {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
      val timestamp = dateFormat.format(new Date())
      val path = s"/app/logs/auditoria_${timestamp}.txt"

      // Cria diretório se não existir
      val logDir = new Path("/app/logs")
      if (!fs.exists(logDir)) {
        fs.mkdirs(logDir)
      }

      val outputPath = new Path(path)
      val stream = fs.create(outputPath)

      outputStream = Some(stream)
      logPath = Some(path)

      log(s"Logger HDFS inicializado. Logs serão salvos em: $path")

    } catch {
      case e: Exception =>
        System.err.println(s"Erro ao inicializar logger HDFS: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def log(message: String): Unit = {
    val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    val logMessage = s"[$timestamp] $message\n"

    // Escreve no console
    println(logMessage.trim)

    // Escreve no HDFS
    outputStream.foreach { stream =>
      try {
        stream.write(logMessage.getBytes("UTF-8"))
        stream.hflush() // Garante que os dados sejam escritos imediatamente
      } catch {
        case e: Exception =>
          System.err.println(s"Erro ao escrever log no HDFS: ${e.getMessage}")
      }
    }
  }

  def logDataFrame(df: org.apache.spark.sql.DataFrame, limit: Int = 10, message: String = ""): Unit = {
    if (message.nonEmpty) {
      log(message)
    }

    try {
      val data = df.take(limit)
      val totalCount = df.count()

      if (data.nonEmpty) {
        log(s"DataFrame com ${df.columns.length} colunas: ${df.columns.mkString(", ")}")
        log("Cabeçalho: " + df.columns.mkString(" | "))
        log("Dados:")
        data.foreach { row =>
          log(row.mkString(" | "))
        }
        log(s"Total de registros no DataFrame: $totalCount")
        log(s"Exibindo ${math.min(limit, totalCount)} registros de $totalCount")
      } else {
        log("DataFrame vazio")
      }
    } catch {
      case e: Exception =>
        log(s"ERRO ao logar DataFrame: ${e.getMessage}")
    }
  }

  def close(): Unit = {
    outputStream.foreach { stream =>
      try {
        log("Finalizando logger HDFS")
        stream.close()
        logPath.foreach(path => println(s"Logs salvos em: $path"))
      } catch {
        case e: Exception =>
          System.err.println(s"Erro ao fechar stream HDFS: ${e.getMessage}")
      }
    }
    outputStream = None
    logPath = None
  }

  def getLogPath: Option[String] = logPath
}

// ========== AuditoriaLogger ==========
object AuditoriaLogger {
  def main(args: Array[String]): Unit = {
    // Default values if not provided in args
    var year = "2026"
    var month = "1"
    var anoInicio = 2026
    var mesInicio = 11
    var anoFim = 2026
    var mesFim = 1

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

    // Inicializa logger HDFS
    HDFSLogger.initialize(spark)

    try {
      HDFSLogger.log("Iniciando processo de auditoria de documentos fiscais")
      HDFSLogger.log(s"Parâmetros recebidos: year=$year, month=$month, anoInicio=$anoInicio, mesInicio=$mesInicio, anoFim=$anoFim, mesFim=$mesFim")

      // Cria diretório de logs se não existir
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val logPath = new Path("/app/logs")
      if (!fs.exists(logPath)) {
        fs.mkdirs(logPath)
        HDFSLogger.log("Diretório /app/logs criado no HDFS")
      }

      // Executa os processos prévios
      HDFSLogger.log("Executando UltimaPastaHDFS")
      UltimaPastaHDFS.main(Array())

      HDFSLogger.log("Executando AgrupamentoParquetPorDia")
      AgrupamentoParquetPorDia.main(Array())

      // Processa os documentos
      HDFSLogger.log(s"Iniciando processamento de documentos para year=$year, month=$month")
      processarTodosDocumentos(spark, year, month)

      // Processa NFE e NFCE
      HDFSLogger.log(s"Iniciando processamento de NFE/NFCE para período: $anoInicio/$mesInicio a $anoFim/$mesFim")
      processarNFeNFCE(spark, anoInicio, mesInicio, anoFim, mesFim)

      HDFSLogger.log("Processo de auditoria concluído com sucesso")

    } catch {
      case e: Exception =>
        HDFSLogger.log(s"ERRO durante a execução da auditoria: ${e.getMessage}")
        HDFSLogger.log(s"Stack trace: ${e.getStackTrace.mkString("\n")}")
        throw e // Re-lança a exceção para não mascarar o erro
    } finally {
      HDFSLogger.close()
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
      ("/datalake/bronze/sources/dbms/dec/diario/bpe_evento/", "/datalake/prata/sources/dbms/dec/bpe/evento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/mdfe_evento/", "/datalake/prata/sources/dbms/dec/mdfe/evento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/nf3e_evento/", "/datalake/prata/sources/dbms/dec/nf3e/evento/", None, None, "NSU"),
      ("/datalake/bronze/sources/dbms/dec/diario/nfcom_evento/", "/datalake/prata/sources/dbms/dec/nfcom/evento/", None, None, "NSU")
    )

    HDFSLogger.log(s"Processando ${documentosConfig.length} tipos de documentos")

    // Processa cada documento
    documentosConfig.zipWithIndex.foreach { case ((bronzePath, prataPath, modelo, filtroXML, colunaVerificacao), index) =>
      HDFSLogger.log(s"Processando documento ${index + 1}/${documentosConfig.length}: $prataPath")

      colunaVerificacao match {
        case "NSUDF" => processarDocumentoComNSUDF(spark, bronzePath, prataPath, year, month, modelo, filtroXML)
        case "NSU" => processarDocumentoComNSU(spark, bronzePath, prataPath, year, month, modelo, filtroXML)
        case "NSUSVD" => processarDocumentoComNSUSVD(spark, bronzePath, prataPath, year, month, modelo, filtroXML)
        case _ => processarDocumento(spark, bronzePath, prataPath, year, month, modelo, filtroXML, colunaVerificacao)
      }
    }

    HDFSLogger.log("Processamento de todos os documentos concluído")
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
    HDFSLogger.log(s"Processando documento: $prataPath")

    try {
      val prataDF = spark.read.parquet(prataPath).select(colunaVerificacao.toLowerCase)
      val chavesRepetidasPrata = prataDF.groupBy(colunaVerificacao.toLowerCase).count().filter(col("count") > 1)

      HDFSLogger.log(s"${colunaVerificacao}s repetidas (quantidade > 1) no prata:")
      HDFSLogger.logDataFrame(chavesRepetidasPrata, 10, s"${colunaVerificacao}s repetidas no prata")

      // Mostra também no console
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
        HDFSLogger.log(s"${colunaVerificacao}s do bronze que não existem no prata para year=${year}/month=${month}:")
        HDFSLogger.logDataFrame(chavesNaoExistentes, 100, s"${colunaVerificacao}s do bronze que não existem no prata")

        // Mostra também no console
        chavesNaoExistentes.show(100, false)

        val countChavesNaoExistentes = chavesNaoExistentes.count()
        HDFSLogger.log(s"Total de ${colunaVerificacao}s não encontradas no prata: $countChavesNaoExistentes")
      } else {
        HDFSLogger.log(s"O caminho $bronzePath não existe.")
      }
    } catch {
      case e: Exception =>
        HDFSLogger.log(s"ERRO ao processar documento $prataPath: ${e.getMessage}")
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
    HDFSLogger.log(s"Processando documento com NSUDF: $prataPath")

    try {
      val prataDF = spark.read.parquet(prataPath).select("nsudf")
      val nsudfsRepetidosPrata = prataDF.groupBy("nsudf").count().filter(col("count") > 1)

      HDFSLogger.log(s"NSUDFs repetidos (quantidade > 1) no prata:")
      HDFSLogger.logDataFrame(nsudfsRepetidosPrata, 10, "NSUDFs repetidos no prata")

      // Mostra também no console
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
        HDFSLogger.log(s"NSUDFs do bronze que não existem no prata para year=${year}/month=${month}:")
        HDFSLogger.logDataFrame(nsudfsNaoExistentes, 100, "NSUDFs do bronze que não existem no prata")

        // Mostra também no console
        nsudfsNaoExistentes.show(100, false)

        val countNsudfsNaoExistentes = nsudfsNaoExistentes.count()
        HDFSLogger.log(s"Total de NSUDFs não encontrados no prata: $countNsudfsNaoExistentes")
      } else {
        HDFSLogger.log(s"O caminho $bronzePath não existe.")
      }
    } catch {
      case e: Exception =>
        HDFSLogger.log(s"ERRO ao processar documento NSUDF $prataPath: ${e.getMessage}")
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
    HDFSLogger.log(s"Processando documento com NSU: $prataPath")

    try {
      val prataDF = spark.read.parquet(prataPath).select("nsu")
      val nsusRepetidosPrata = prataDF.groupBy("nsu").count().filter(col("count") > 1)

      HDFSLogger.log(s"NSUs repetidos (quantidade > 1) no prata:")
      HDFSLogger.logDataFrame(nsusRepetidosPrata, 10, "NSUs repetidos no prata")

      // Mostra também no console
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
        HDFSLogger.log(s"NSUs do bronze que não existem no prata para year=${year}/month=${month}:")
        HDFSLogger.logDataFrame(nsusNaoExistentes, 100, "NSUs do bronze que não existem no prata")

        // Mostra também no console
        nsusNaoExistentes.show(100, false)

        val countNsusNaoExistentes = nsusNaoExistentes.count()
        HDFSLogger.log(s"Total de NSUs não encontrados no prata: $countNsusNaoExistentes")
      } else {
        HDFSLogger.log(s"O caminho $bronzePath não existe.")
      }
    } catch {
      case e: Exception =>
        HDFSLogger.log(s"ERRO ao processar documento NSU $prataPath: ${e.getMessage}")
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
    HDFSLogger.log(s"Processando documento com NSUSVD: $prataPath")

    try {
      val prataDF = spark.read.parquet(prataPath).select("nsusvd")
      val nsusvdRepetidosPrata = prataDF.groupBy("nsusvd").count().filter(col("count") > 1)

      HDFSLogger.log(s"NSUSVDs repetidos (quantidade > 1) no prata:")
      HDFSLogger.logDataFrame(nsusvdRepetidosPrata, 10, "NSUSVDs repetidos no prata")

      // Mostra também no console
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
        HDFSLogger.log(s"NSUSVDs do bronze que não existem no prata para year=${year}/month=${month}:")
        HDFSLogger.logDataFrame(nsusvdNaoExistentes, 100, "NSUSVDs do bronze que não existem no prata")

        // Mostra também no console
        nsusvdNaoExistentes.show(100, false)

        val countNsusvdNaoExistentes = nsusvdNaoExistentes.count()
        HDFSLogger.log(s"Total de NSUSVDs não encontrados no prata: $countNsusvdNaoExistentes")
      } else {
        HDFSLogger.log(s"O caminho $bronzePath não existe.")
      }
    } catch {
      case e: Exception =>
        HDFSLogger.log(s"ERRO ao processar documento NSUSVD $prataPath: ${e.getMessage}")
    }
  }

  private def processarNFeNFCE(spark: SparkSession, anoInicio: Int, mesInicio: Int, anoFim: Int, mesFim: Int): Unit = {
    // Configura a compactação LZ4
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    try {
      // Para NFE
      HDFSLogger.log("Iniciando processamento de NFE")
      AuditoriaDet.identificarChavesFaltantesNoPrata(spark, "nfe", anoInicio, mesInicio, anoFim, mesFim)
      AuditoriaDet.identificarAusencias(spark, "nfe")
      AuditoriaDet.verificarDuplicidade(spark, "nfe")
      HDFSLogger.log("Processamento de NFE concluído")

      // Para NFCE
      HDFSLogger.log("Iniciando processamento de NFCE")
      AuditoriaDet.identificarChavesFaltantesNoPrata(spark, "nfce", anoInicio, mesInicio, anoFim, mesFim)
      AuditoriaDet.identificarAusencias(spark, "nfce")
      AuditoriaDet.verificarDuplicidade(spark, "nfce")
      HDFSLogger.log("Processamento de NFCE concluído")

    } catch {
      case e: Exception =>
        HDFSLogger.log(s"ERRO durante processamento de NFE/NFCE: ${e.getMessage}")
    }
  }
}
//AuditoriaLogger.main(Array())