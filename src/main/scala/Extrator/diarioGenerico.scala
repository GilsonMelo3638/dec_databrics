package Extrator

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Properties

object diarioGenerico {

  def processDocument(
                       spark: SparkSession, // Sessão do Spark reutilizada
                       jdbcUrl: String, // URL de conexão com o banco de dados
                       connectionProperties: Properties, // Propriedades de conexão
                       documentType: String, // Tipo de documento (BPe, CTe, etc.)
                       splitByColumn: String, // Coluna de particionamento
                       targetDirBase: String // Caminho base de destino no HDFS
                     ): Unit = {
    try {
      // Obtém a data de ontem no fuso horário desejado
      val ontem = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(1)

      // Formata as variáveis conforme necessário
      val formatterAnoMes = DateTimeFormatter.ofPattern("yyyyMM")
      val formatterAnoMesDia = DateTimeFormatter.ofPattern("yyyyMMdd")
      val formatterDataHora = DateTimeFormatter.ofPattern("dd/MM/yyyy")

      val anoMes = ontem.format(formatterAnoMes) // Formato: "202502"
      val anoMesDia = ontem.format(formatterAnoMesDia) // Formato: "20250201"
      val dataFormatada = ontem.format(formatterDataHora)

      val dataInicial = s"$dataFormatada 00:00:00"
      val dataFinal = s"$dataFormatada 23:59:59"

      // Exibe as variáveis
      println(s"anoMes: $anoMes")
      println(s"anoMesDia: $anoMesDia")
      println(s"dataInicial: $dataInicial")
      println(s"dataFinal: $dataFinal")

      // Define o caminho de destino no HDFS
      val targetDir = s"$targetDirBase/$documentType/processar/$anoMesDia"

      // Define o diretório de processado com base no tipo de documento
      val processedDir = documentType match {
        case "BPe" => s"$targetDirBase/bpe/processado/$anoMesDia"
        case "CTe" => s"$targetDirBase/cte/processado/$anoMesDia"
        case "MDFe" => s"$targetDirBase/mdfe/processado/$anoMesDia"
        case "NF3e" => s"$targetDirBase/nf3e/processado/$anoMesDia"
        case "NFCom" => s"$targetDirBase/nfcom/processado/$anoMesDia"
        case _ => throw new IllegalArgumentException(s"Tipo de documento não suportado: $documentType")
      }

      // Verifica se o arquivo já existe no diretório de processado
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val processedPath = new Path(processedDir)
      if (fs.exists(processedPath)) {
        println(s"Arquivo $anoMesDia já existe no diretório de processado: $processedDir. Nada a ser feito.")
        return // Interrompe a execução
      }

      // Verifica se o diretório de destino já existe no HDFS
      val targetPath = new Path(targetDir)
      if (fs.exists(targetPath)) {
        println(s"Diretório $targetDir já existe. Dados para o período $anoMesDia já foram gravados.")
        return // Interrompe a execução
      }

      // Número de partições (equivalente ao --num-mappers do Sqoop)
      val numPartitions = 20

      // Query SQL base
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

      // Obtém os valores mínimo e máximo da coluna de particionamento como java.math.BigDecimal
      val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
      val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

      // Converte java.math.BigDecimal para Double
      val min = minMaxDF.select("min").first().getAs[java.math.BigDecimal](0).doubleValue()
      val max = minMaxDF.select("max").first().getAs[java.math.BigDecimal](0).doubleValue()

      // Cria as partições com base no intervalo de valores da coluna de particionamento
      val step = ((max - min) / numPartitions).toLong
      val partitionBounds = if (step == 0) {
        // Se há apenas um valor, cria partições com passo 1
        (min.toLong to max.toLong by 1L).toList
      } else {
        (min.toLong to max.toLong by step).toList
      }
      // Carrega os dados do Oracle com particionamento
      val df = spark.read.jdbc(
        jdbcUrl,
        s"($baseQuery) tmp",
        splitByColumn, // Coluna de particionamento
        partitionBounds.head, // Valor mínimo
        partitionBounds.last, // Valor máximo
        numPartitions, // Número de partições
        connectionProperties
      )

      // Salva os dados no HDFS no formato Parquet com compressão LZ4
      df.write
        .option("compression", "lz4")
        .parquet(targetDir)

      // Marca o processamento como concluído criando o diretório de processado
      fs.mkdirs(processedPath)
      println(s"=== Processamento de $documentType concluído com sucesso ===")
    } catch {
      case e: Exception =>
        println(s"Erro ao processar $documentType: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
//diarioGenerico.main(Array())