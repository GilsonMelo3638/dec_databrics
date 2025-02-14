package Sqoop
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object SqoopNFePorHoraProcessorIncremental {

  def main(args: Array[String]): Unit = {
    while (true) {
      val agora = LocalDateTime.now(ZoneId.of("America/Sao_Paulo"))

      if (agora.getMinute == 15) {
        val spark = SparkSession.builder()
          .appName("SqoopToSparkWithPartitioning")
          .config("spark.yarn.queue", "workloads")
          .getOrCreate()

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val horaAnterior = agora.minusHours(1)

        processarHora(spark, fs, horaAnterior)

        // Verifica as lacunas nos últimos dois dias
        val doisDiasAtras = agora.minusDays(2)
        val horasFaltantes = identificarHorasFaltantes(fs, doisDiasAtras, agora)

        // Processa as horas faltantes
        horasFaltantes.foreach { horaFaltante =>
          processarHoraFaltante(spark, fs, horaFaltante)
        }

        spark.stop()
      }

      Thread.sleep(60000)
    }
  }

  def processarHora(spark: SparkSession, fs: FileSystem, hora: LocalDateTime): Unit = {
    val jdbcUrl = "jdbc:oracle:thin:@sefsrvprd704.fazenda.net:1521/ORAPRD21"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "userdec")
    connectionProperties.put("password", "userdec201811")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    val splitByColumn = "NSUDF"
    val numPartitions = 200

    val (dataInicial, dataFinal) = formatarIntervalo(hora)

    val baseQuery = construirQuery(dataInicial, dataFinal)
    val df = lerDataFrame(spark, jdbcUrl, baseQuery, splitByColumn, numPartitions, connectionProperties, dataInicial, dataFinal)

    if (df.isDefined) {
      val dfComParticoes = adicionarParticoes(df.get, hora)
      val targetDir = construirDiretorio(hora)
      escreverDataFrame(dfComParticoes, fs, targetDir)
    }
  }

  def processarHoraFaltante(spark: SparkSession, fs: FileSystem, horaFaltante: LocalDateTime): Unit = {
    processarHora(spark, fs, horaFaltante)
  }

  def formatarIntervalo(hora: LocalDateTime): (String, String) = {
    val formatterData = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val formatterHora = DateTimeFormatter.ofPattern("HH")

    val dataFormatada = hora.format(formatterData)
    val horaFormatada = hora.format(formatterHora)

    val dataInicial = s"$dataFormatada $horaFormatada:00:00"
    val dataFinal = s"$dataFormatada $horaFormatada:59:59"

    (dataInicial, dataFinal)
  }

  def construirQuery(dataInicial: String, dataFinal: String): String = {
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
  }

  def lerDataFrame(spark: SparkSession, jdbcUrl: String, baseQuery: String, splitByColumn: String, numPartitions: Int, connectionProperties: Properties, dataInicial: String, dataFinal: String): Option[DataFrame] = {
    val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
    val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

    val minRow = minMaxDF.select("min").first()
    val maxRow = minMaxDF.select("max").first()

    if (minRow.isNullAt(0) || maxRow.isNullAt(0)) {
      println(s"Não há dados para o intervalo de tempo $dataInicial - $dataFinal. Pulando o processamento.")
      None
    } else {
      val min = minRow.getAs[java.math.BigDecimal](0).doubleValue()
      val max = maxRow.getAs[java.math.BigDecimal](0).doubleValue()

      val partitionBounds = (min.toLong to max.toLong by ((max - min) / numPartitions).toLong).toList

      val df = spark.read.jdbc(
        jdbcUrl,
        s"($baseQuery) tmp",
        splitByColumn,
        partitionBounds.head,
        partitionBounds.last,
        numPartitions,
        connectionProperties
      )

      Some(df)
    }
  }

  def adicionarParticoes(df: DataFrame, hora: LocalDateTime): DataFrame = {
    df
      .withColumn("ano", lit(hora.getYear)) // Extrai o ano
      .withColumn("mes", lit(hora.getMonthValue)) // Extrai o mês
      .withColumn("dia", lit(hora.getDayOfMonth)) // Extrai o dia
      .withColumn("hora", lit(hora.getHour)) // Extrai a hora
  }

  def construirDiretorio(hora: LocalDateTime): String = {
    s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/ano=${hora.getYear}/mes=${hora.getMonthValue}/dia=${hora.getDayOfMonth}/hora=${hora.getHour}"
  }

  def escreverDataFrame(df: DataFrame, fs: FileSystem, targetDir: String): Unit = {
    val path = new Path(targetDir)
    val dfReparticionado = df.repartition(4, col("ano"), col("mes"), col("dia"), col("hora"))
    if (!fs.exists(path)) {
      dfReparticionado.write
        .option("compression", "lz4")
        .parquet(targetDir)
    } else {
      println(s"O diretório $targetDir já existe. Pulando a escrita.")
    }
  }

  def identificarHorasFaltantes(fs: FileSystem, inicio: LocalDateTime, fim: LocalDateTime): List[LocalDateTime] = {
    val formatterAnoMesDiaHora = DateTimeFormatter.ofPattern("yyyyMMddHH")
    val horasFaltantes = scala.collection.mutable.ListBuffer.empty[LocalDateTime]

    var horaAtual = inicio
    while (horaAtual.isBefore(fim)) {
      val anoMesDiaHora = horaAtual.format(formatterAnoMesDiaHora)
      val path = new Path(s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/ano=${horaAtual.getYear}/mes=${horaAtual.getMonthValue}/dia=${horaAtual.getDayOfMonth}/hora=${horaAtual.getHour}")

      if (!fs.exists(path)) {
        horasFaltantes += horaAtual
      }

      horaAtual = horaAtual.plusHours(1)
    }

    horasFaltantes.toList
  }
}
// SqoopNFePorHoraProcessorIncremental.main(Array())