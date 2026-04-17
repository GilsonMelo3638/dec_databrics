package Extrator

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Properties

object diarioGenerico2 {

  def processDocument(
                       spark: SparkSession,
                       jdbcUrl: String,
                       connectionProperties: Properties,
                       documentType: String,
                       splitByColumn: String,
                       targetDirBase: String
                     ): Unit = {

    try {

      val ontem = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(1)

      val formatterAnoMes = DateTimeFormatter.ofPattern("yyyyMM")
      val formatterAnoMesDia = DateTimeFormatter.ofPattern("yyyyMMdd")
      val formatterDataHora = DateTimeFormatter.ofPattern("dd/MM/yyyy")

      val anoMes = ontem.format(formatterAnoMes)
      val anoMesDia = ontem.format(formatterAnoMesDia)
      val dataFormatada = ontem.format(formatterDataHora)

      val dataInicial = s"$dataFormatada 00:00:00"
      val dataFinal = s"$dataFormatada 23:59:59"

      println(s"anoMes: $anoMes")
      println(s"anoMesDia: $anoMesDia")

      val targetDir = s"$targetDirBase/$documentType/processar/$anoMesDia"

      val processedDir = documentType match {
        case "BPe"  => s"$targetDirBase/bpe/processado/$anoMesDia"
        case "CTe"  => s"$targetDirBase/cte/processado/$anoMesDia"
        case "MDFe" => s"$targetDirBase/mdfe/processado/$anoMesDia"
        case "NF3e" => s"$targetDirBase/nf3e/processado/$anoMesDia"
        case "NFCom"=> s"$targetDirBase/nfcom/processado/$anoMesDia"
        case _ => throw new IllegalArgumentException(s"Tipo inválido: $documentType")
      }

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      if (fs.exists(new Path(processedDir))) {
        println(s"Já processado: $processedDir")
        return
      }

      if (fs.exists(new Path(targetDir))) {
        println(s"Já existe destino: $targetDir")
        return
      }

      val baseQuery = documentType match {
      case "BPe" =>
      s"""
        SELECT NSU,
        REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//bpeProc', 'xmlns=\"http://www.portalfiscal.inf.br/bpe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
               f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
               TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
               TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
               TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
               f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
        FROM DEC_DFE_BPE f
        WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
      """
      case "CTe" =>
      s"""
        SELECT NSUSVD,
          COALESCE(
                      REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//cteSimpProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
                      REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//cteProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
                      REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//cteOSProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
                      REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//GTVeProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' ')
                  ) AS XML_DOCUMENTO_CLOB,
          f.NSUAUT,
          f.CSTAT,
          f.CHAVE,
          f.IP_TRANSMISSOR,
          TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
          TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
          TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
          f.EMITENTE,
          f.UF_EMITENTE,
          f.DESTINATARIO,
          f.UF_DESTINATARIO,
          f.MODELO,
          f.TPEMIS
          FROM DEC_DFE_CTE_SVD f
        WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
      """
      case "MDFe" =>
      s"""
        SELECT NSU,
        REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//mdfeProc', 'xmlns=\"http://www.portalfiscal.inf.br/mdfe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
               f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
               TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
               TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
               TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
               f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
        FROM DEC_DFE_MDFE f
        WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
      """
      case "NF3e" =>
      s"""
        SELECT NSU,
        REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nf3eProc', 'xmlns=\"http://www.portalfiscal.inf.br/nf3e\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
               f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
               TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
               TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
               TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
               f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
        FROM DEC_DFE_NF3E f
        WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
      """

      case "NFCom" =>
      s"""
        SELECT NSU,
          COALESCE(
                      REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//NFComProc', 'xmlns=\"http://www.portalfiscal.inf.br/nfcom\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
                      REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nfcomProc', 'xmlns=\"http://www.portalfiscal.inf.br/nfcom\"') AS CLOB), CHR(10), ' '), CHR(13), ' ')
                  ) AS XML_DOCUMENTO_CLOB,
               f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
               TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
               TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
               TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
               f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
        FROM DEC_DFE_NFCOM f
        WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
      """
      case _ =>
      throw new IllegalArgumentException(s"Tipo de documento não suportado: $documentType")
      }

      // ================================
      // 🔥 CONTAGEM DE LINHAS
      // ================================
      val countQuery = s"(SELECT COUNT(*) AS total FROM ($baseQuery)) tmp"

      val rowCount = spark.read
        .jdbc(jdbcUrl, countQuery, connectionProperties)
        .first()
        .getLong(0)

      println(s"Total de linhas: $rowCount")

      if (rowCount == 0) {
        println("Sem dados.")
        return
      }

      // ================================
      // 🔥 ESTIMATIVA DE TAMANHO
      // ================================
      val avgRowSizeBytes = 3000 // ajuste fino depois se quiser

      val estimatedSize = rowCount * avgRowSizeBytes

      val targetFileSize = 256L * 1024 * 1024 // 256MB

      val idealPartitions = Math.max(1, (estimatedSize / targetFileSize).toInt)

      val maxPartitions = 100

      val numPartitions = Math.min(idealPartitions, maxPartitions)

      println(s"Partições calculadas: $numPartitions")

      // ================================
      // 🔥 MIN/MAX PARA JDBC
      // ================================
      val minMaxQuery = s"(SELECT MIN($splitByColumn) min, MAX($splitByColumn) max FROM ($baseQuery)) tmp"

      val bounds = spark.read.jdbc(jdbcUrl, minMaxQuery, connectionProperties).first()

      val min = bounds.getDecimal(0).longValue()
      val max = bounds.getDecimal(1).longValue()

      // ================================
      // 🔥 LEITURA JDBC
      // ================================
      val df = spark.read.jdbc(
        jdbcUrl,
        s"($baseQuery) tmp",
        splitByColumn,
        min,
        max,
        numPartitions,
        connectionProperties
      )

      // ================================
      // 🔥 AJUSTE FINAL DE PARTIÇÕES
      // ================================
      val finalDF: DataFrame =
        if (numPartitions < df.rdd.getNumPartitions)
          df.coalesce(numPartitions)
        else
          df.repartition(numPartitions)

      // ================================
      // 🔥 CONTROLE DE TAMANHO DE ARQUIVO
      // ================================
      spark.conf.set("spark.sql.files.maxPartitionBytes", 256L * 1024 * 1024)

      // ================================
      // 🔥 ESCRITA
      // ================================
      finalDF.write
        .option("compression", "lz4")
        .mode("overwrite")
        .parquet(targetDir)

      fs.mkdirs(new Path(processedDir))

      println(s"✅ Finalizado com sucesso: $documentType")

    } catch {
      case e: Exception =>
        println(s"Erro: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}