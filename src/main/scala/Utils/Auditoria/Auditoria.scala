package Auditoria

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.hadoop.fs.{FileSystem, Path}

object Auditoria{
  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("AuditoriaDocumentosFiscais")
      .getOrCreate()

    try {
      // Executa os processos prévios
      UltimaPastaHDFS.main(Array())
      AgrupamentoParquetPorMes.main(Array())

      // Processa os documentos
      processarTodosDocumentos(spark)

      // Processa NFE e NFCE
      processarNFeNFCE(spark)

    } finally {
      spark.close()
    }
  }

  private def processarTodosDocumentos(spark: SparkSession): Unit = {
    val anoMesList = Seq("202503")

    // Lista de configurações para cada tipo de documento
    val documentosConfig = List(
      ("/datalake/bronze/sources/dbms/dec/bpe/", "/datalake/prata/sources/dbms/dec/bpe/BPe/", None, None),
      ("/datalake/bronze/sources/dbms/dec/mdfe/", "/datalake/prata/sources/dbms/dec/mdfe/MDFe/", None, None),
      ("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/CTeSimp/", Some(57), Some("<cteSimpProc")),
      ("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/CTe/", Some(57), Some("<cteProc")),
      ("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/CTeOS/", Some(67), None),
      ("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/GVTe/", Some(64), None),
      ("/datalake/bronze/sources/dbms/dec/nf3e/", "/datalake/prata/sources/dbms/dec/nf3e/NF3e/", None, None),
      ("/datalake/bronze/sources/dbms/dec/bpe_cancelamento/", "/datalake/prata/sources/dbms/dec/bpe/cancelamento/", None, None),
      ("/datalake/bronze/sources/dbms/dec/nf3e_cancelamento/", "/datalake/prata/sources/dbms/dec/nf3e/cancelamento/", None, None),
      ("/datalake/bronze/sources/dbms/dec/nfce_cancelamento/", "/datalake/prata/sources/dbms/dec/nfce/cancelamento/", None, None),
      ("/datalake/bronze/sources/dbms/dec/mdfe_cancelamento/", "/datalake/prata/sources/dbms/dec/mdfe/cancelamento/", None, None),
      ("/datalake/bronze/sources/dbms/dec/nfe_cancelamento/", "/datalake/prata/sources/dbms/dec/nfe/cancelamento/", None, None),
      ("/datalake/bronze/sources/dbms/dec/cte_cancelamento/", "/datalake/prata/sources/dbms/dec/cte/cancelamento/", None, None),
      ("/datalake/bronze/sources/dbms/dec/nfe/", "/datalake/prata/sources/dbms/dec/nfe/infNFe/", None, None),
      ("/datalake/bronze/sources/dbms/dec/nfce/", "/datalake/prata/sources/dbms/dec/nfce/infNFCe/", None, None)
    )

    // Processa cada documento
    documentosConfig.foreach { case (bronzePath, prataPath, modelo, filtroXML) =>
      processarDocumento(spark, bronzePath, prataPath, anoMesList, modelo, filtroXML)
    }
  }

  private def processarDocumento(
                                  spark: SparkSession,
                                  bronzeBasePath: String,
                                  prataPath: String,
                                  anoMesList: Seq[String],
                                  modelo: Option[Int] = None,
                                  filtroXML: Option[String] = None
                                ): Unit = {
    println(s"\nProcessando documento: $prataPath")

    val prataDF = spark.read.parquet(prataPath).select("chave")
    val chavesRepetidasPrata = prataDF.groupBy("chave").count().filter(col("count") > 1)

    println(s"Chaves repetidas (quantidade > 1) no prata:")
    chavesRepetidasPrata.show(10, false)

    anoMesList.foreach { anoMes =>
      val bronzePath = s"${bronzeBasePath}${anoMes}"
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
        bronzeDF = bronzeDF.select(col("CHAVE").alias("chave")).distinct()

        val chavesNaoExistentes = bronzeDF.except(prataDF)
        println(s"Chaves do bronze que não existem no prata para $anoMes:")
        chavesNaoExistentes.show(100, false)
      } else {
        println(s"O caminho $bronzePath não existe.")
      }
    }
  }

  private def processarNFeNFCE(spark: SparkSession): Unit = {
    // Configura a compactação LZ4
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    // Para NFE
    println("\nProcessando NFE:")
    AuditoriaDet.identificarChavesFaltantesNoPrata(spark, "nfe", 2025, 3, 2025, 3)
    AuditoriaDet.identificarAusencias(spark, "nfe")
    AuditoriaDet.verificarDuplicidade(spark, "nfe")

    // Para NFCE
    println("\nProcessando NFCE:")
    AuditoriaDet.identificarChavesFaltantesNoPrata(spark, "nfce", 2025, 3, 2025, 3)
    AuditoriaDet.identificarAusencias(spark, "nfce")
    AuditoriaDet.verificarDuplicidade(spark, "nfce")
  }
}
//Auditoria.main(Array())