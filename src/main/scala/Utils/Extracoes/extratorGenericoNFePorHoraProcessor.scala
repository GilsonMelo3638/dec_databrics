package Utils.Extracoes

import org.apache.spark.sql.SparkSession
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

object extratorGenericoNFePorHoraProcessor {

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("ExtratorToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    // Obtém a hora atual no fuso horário desejado e ajusta para a hora anterior
    val agora = LocalDateTime.now(ZoneId.of("America/Sao_Paulo")).minusHours(1)

    // Formata as variáveis conforme necessário
    val formatterAnoMesDiaHora = DateTimeFormatter.ofPattern("yyyyddMMHH")
    val formatterData = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val formatterHora = DateTimeFormatter.ofPattern("HH")

    val anoMesDiaHora = agora.format(formatterAnoMesDiaHora) // Formato: "2025100210"
    val dataFormatada = agora.format(formatterData)         // Formato: "10/02/2025"
    val horaFormatada = agora.format(formatterHora)         // Formato: "10"

    val dataInicial = s"$dataFormatada $horaFormatada:00:00"
    val dataFinal = s"$dataFormatada $horaFormatada:59:59"

    // Exibe as variáveis
    println(s"anoMesDiaHora: $anoMesDiaHora")
    println(s"dataInicial: $dataInicial")
    println(s"dataFinal: $dataFinal")

    // Configurações de conexão com o banco de dados Oracle
    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admhadoop")
    connectionProperties.put("password", ".admhadoop#")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    // Coluna para particionamento
    val splitByColumn = "NSUDF"
    val numPartitions = 200

    // Query SQL base
    val baseQuery =
      s"""
    SELECT NSUDF,
           REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nfeProc', 'xmlns=\"http://www.portalfiscal.inf.br/nfe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
           f.NSUAN,f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
           TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
           TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
           TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
           f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
    FROM DEC_DFE_NFE f
    WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
  """

    // Obtém os valores mínimo e máximo da coluna de particionamento
    val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
    val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

    val min = minMaxDF.select("min").first().getAs[java.math.BigDecimal](0).doubleValue()
    val max = minMaxDF.select("max").first().getAs[java.math.BigDecimal](0).doubleValue()

    val partitionBounds = (min.toLong to max.toLong by ((max - min) / numPartitions).toLong).toList

    // Carrega os dados do Oracle com particionamento
    val df = spark.read.jdbc(
      jdbcUrl,
      s"($baseQuery) tmp",
      splitByColumn,
      partitionBounds.head,
      partitionBounds.last,
      numPartitions,
      connectionProperties
    )

    // Define o caminho de destino no HDFS
    val targetDir = s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/$anoMesDiaHora"

    // Salva os dados no HDFS no formato Parquet com compressão LZ4
    df.write
      .option("compression", "lz4")
      .parquet(targetDir)
  }
}

// extratorGenericoNFePorHoraProcessor.main(Array())
