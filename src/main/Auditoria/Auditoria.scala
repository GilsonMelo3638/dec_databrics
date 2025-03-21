package Auditoria

import Auditoria.{AgrupamentoParquetPorMes, AuditoriaDet, UltimaPastaHDFS}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.hadoop.fs.{FileSystem, Path}

UltimaPastaHDFS.main(Array())
AgrupamentoParquetPorMes.main(Array())

// Função genérica para processar os dados
def processarDocumento(bronzeBasePath: String, prataPath: String, anoMesList: Seq[String], modelo: Option[Int] = None, filtroXML: Option[String] = None): Unit = {
  val prataDF = spark.read.parquet(prataPath).select("chave")
  val chavesRepetidasPrata = prataDF.groupBy("chave").count().filter(col("count") > 1)

  println(s"Chaves repetidas (quantidade > 1) no prata:")
  chavesRepetidasPrata.show(100, false)

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

// Lista de AnoMes específicos
val anoMesList = Seq("202503")

// Chamar as funções para cada tipo de documento
processarDocumento("/datalake/bronze/sources/dbms/dec/bpe/", "/datalake/prata/sources/dbms/dec/bpe/BPe/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/mdfe/", "/datalake/prata/sources/dbms/dec/mdfe/MDFe/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/CTeSimp/", anoMesList, Some(57), Some("<cteSimpProc"))
processarDocumento("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/CTe/", anoMesList, Some(57), Some("<cteProc"))
processarDocumento("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/CTeOS/", anoMesList, Some(67))
processarDocumento("/datalake/bronze/sources/dbms/dec/cte/", "/datalake/prata/sources/dbms/dec/cte/GVTe/", anoMesList, Some(64))
processarDocumento("/datalake/bronze/sources/dbms/dec/nf3e/", "/datalake/prata/sources/dbms/dec/nf3e/NF3e/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/bpe_cancelamento/", "/datalake/prata/sources/dbms/dec/bpe/cancelamento/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/nf3e_cancelamento/", "/datalake/prata/sources/dbms/dec/nf3e/cancelamento/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/nfce_cancelamento/", "/datalake/prata/sources/dbms/dec/nfce/cancelamento/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/mdfe_cancelamento/", "/datalake/prata/sources/dbms/dec/mdfe/cancelamento/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/nfe_cancelamento/", "/datalake/prata/sources/dbms/dec/nfe/cancelamento/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/nfe/", "/datalake/prata/sources/dbms/dec/nfe/infNFe/", anoMesList)
processarDocumento("/datalake/bronze/sources/dbms/dec/nfce/", "/datalake/prata/sources/dbms/dec/nfce/infNFCe/", anoMesList)


val spark = SparkSession.builder.appName("AuditoriaDet").getOrCreate()

// Para NFE
val processorNFe = new AuditoriaDet(spark, "nfe")
processorNFe.identificarChavesFaltantesNoPrata(2025, 3, 2025, 3)
processorNFe.identificarAusencias()
processorNFe.verificarDuplicidade()

// Para NFCE
val processorNFCe = new AuditoriaDet(spark, "nfce")
processorNFCe.identificarChavesFaltantesNoPrata(2025, 3, 2025, 3)
processorNFCe.identificarAusencias()
processorNFCe.verificarDuplicidade()
spark.close()