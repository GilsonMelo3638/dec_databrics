package Sqoop
import org.apache.spark.sql.SparkSession
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object SqoopNFePorHoraProcessorIncremental {

  def main(args: Array[String]): Unit = {
    while (true) {
      val agora = LocalDateTime.now(ZoneId.of("America/Sao_Paulo"))

      if (agora.getMinute == 55) {
        val spark = SparkSession.builder()
          .appName("SqoopToSparkWithPartitioning")
          .config("spark.yarn.queue", "workloads")
          .getOrCreate()

        // Inicializa o FileSystem
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val horaAnterior = agora.minusHours(1)

        val formatterAnoMesDiaHora = DateTimeFormatter.ofPattern("yyyyMMddHH")
        val formatterData = DateTimeFormatter.ofPattern("dd/MM/yyyy")
        val formatterHora = DateTimeFormatter.ofPattern("HH")

        val anoMesDiaHoraAtual = horaAnterior.format(formatterAnoMesDiaHora)
        val dataFormatada = horaAnterior.format(formatterData)
        val horaFormatada = horaAnterior.format(formatterHora)

        val dataInicial = s"$dataFormatada $horaFormatada:00:00"
        val dataFinal = s"$dataFormatada $horaFormatada:59:59"

        println(s"anoMesDiaHora: $anoMesDiaHoraAtual")
        println(s"dataInicial: $dataInicial")
        println(s"dataFinal: $dataFinal")

        val jdbcUrl = "jdbc:oracle:thin:@sefsrvprd704.fazenda.net:1521/ORAPRD21"
        val connectionProperties = new Properties()
        connectionProperties.put("user", "userdec")
        connectionProperties.put("password", "userdec201811")
        connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

        val splitByColumn = "NSUDF"
        val numPartitions = 200

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

        val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
        val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

        val minRow = minMaxDF.select("min").first()
        val maxRow = minMaxDF.select("max").first()

        if (minRow.isNullAt(0) || maxRow.isNullAt(0)) {
          println("Não há dados para o intervalo de tempo especificado. Pulando o processamento.")
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

          // Adiciona colunas de partição (ano, mês, dia, hora)
          val dfComParticoes = df
            .withColumn("ano", lit(horaAnterior.getYear)) // Extrai o ano
            .withColumn("mes", lit(horaAnterior.getMonthValue)) // Extrai o mês
            .withColumn("dia", lit(horaAnterior.getDayOfMonth)) // Extrai o dia
            .withColumn("hora", lit(horaAnterior.getHour)) // Extrai a hora

          // Define o diretório de destino com base nas partições
          val targetDir = s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/ano=${horaAnterior.getYear}/mes=${horaAnterior.getMonthValue}/dia=${horaAnterior.getDayOfMonth}/hora=${horaAnterior.getHour}"
          val path = new Path(targetDir)

          if (!fs.exists(path)) {
            // Salva o DataFrame com partições de ano, mês, dia e hora
            dfComParticoes.write
              .option("compression", "lz4")
              .parquet(targetDir)
          } else {
            println(s"O diretório $targetDir já existe. Pulando a escrita.")
          }
        }

        // Verifica as lacunas nos últimos dois dias
        val doisDiasAtras = agora.minusDays(2)
        val horasFaltantes = identificarHorasFaltantes(fs, doisDiasAtras, agora)

        // Processa as horas faltantes
        horasFaltantes.foreach { horaFaltante =>
          processarHoraFaltante(spark, fs, horaFaltante, jdbcUrl, connectionProperties, splitByColumn, numPartitions)
        }

        spark.stop()
      }

      Thread.sleep(60000)
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

  def processarHoraFaltante(spark: SparkSession, fs: FileSystem, horaFaltante: LocalDateTime, jdbcUrl: String, connectionProperties: Properties, splitByColumn: String, numPartitions: Int): Unit = {
    val formatterAnoMesDiaHora = DateTimeFormatter.ofPattern("yyyyMMddHH")
    val formatterData = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val formatterHora = DateTimeFormatter.ofPattern("HH")

    val anoMesDiaHora = horaFaltante.format(formatterAnoMesDiaHora)
    val dataFormatada = horaFaltante.format(formatterData)
    val horaFormatada = horaFaltante.format(formatterHora)

    val dataInicial = s"$dataFormatada $horaFormatada:00:00"
    val dataFinal = s"$dataFormatada $horaFormatada:59:59"

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

    val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
    val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

    val minRow = minMaxDF.select("min").first()
    val maxRow = minMaxDF.select("max").first()

    if (minRow.isNullAt(0) || maxRow.isNullAt(0)) {
      println(s"Não há dados para o intervalo de tempo $dataInicial - $dataFinal. Pulando o processamento.")
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

      // Adiciona colunas de partição (ano, mês, dia, hora)
      val dfComParticoes = df
        .withColumn("ano", lit(horaFaltante.getYear)) // Extrai o ano
        .withColumn("mes", lit(horaFaltante.getMonthValue)) // Extrai o mês
        .withColumn("dia", lit(horaFaltante.getDayOfMonth)) // Extrai o dia
        .withColumn("hora", lit(horaFaltante.getHour)) // Extrai a hora

      // Define o diretório de destino com base nas partições
      val targetDir = s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/ano=${horaFaltante.getYear}/mes=${horaFaltante.getMonthValue}/dia=${horaFaltante.getDayOfMonth}/hora=${horaFaltante.getHour}"
      val path = new Path(targetDir)

      if (!fs.exists(path)) {
        // Salva o DataFrame com partições de ano, mês, dia e hora
        dfComParticoes.write
          .option("compression", "lz4")
          .parquet(targetDir)
      } else {
        println(s"O diretório $targetDir já existe. Pulando a escrita.")
      }
    }
  }
}
// SqoopNFePorHoraProcessorIncremental.main(Array())