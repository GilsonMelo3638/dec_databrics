package Extrator

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

/**
 * Objeto responsável pelo processamento incremental de dados de NFe
 * utilizando Spark e integração com Oracle via JDBC.
 */
object extratorNFePorHoraProcessorIncremental {
  /**
   * Método principal que executa o processamento em loop contínuo.
   * Ele verifica a cada minuto se o horário atual corresponde ao minuto 15.
   * Se for o caso, inicia o processamento dos dados da última hora e verifica
   * se há horas faltantes nos últimos dois dias para processá-las.
   */
  def main(args: Array[String]): Unit = {
    while (true) {
      val agora = LocalDateTime.now(ZoneId.of("America/Sao_Paulo")) // Obtém a data e hora atual no fuso horário de São Paulo

      if (agora.getMinute == 15) {// Executa o processamento somente quando for o minuto 15 de cada hora
        val spark = SparkSession.builder()
          .appName("ExtratorToSparkWithPartitioning")
          .config("spark.yarn.queue", "workloads") // Define a fila de execução no YARN
          .getOrCreate()

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration) // Obtém a configuração do sistema de arquivos Hadoop
        val horaAnterior = agora.minusHours(1) // Determina a hora anterior para processamento

        processarHora(spark, fs, horaAnterior) // Processa os dados da última hora

        // Verifica se há horas faltantes nos últimos dois dias
        val doisDiasAtras = agora.minusDays(2)
        val horasFaltantes = identificarHorasFaltantes(fs, doisDiasAtras, agora)

        // Processa as horas que estão faltando
        horasFaltantes.foreach { horaFaltante =>
          processarHoraFaltante(spark, fs, horaFaltante)
        }

        spark.stop()
      }

      Thread.sleep(60000)  // Aguarda 1 minuto antes de verificar novamente
    }
  }

  /**
   * Processa os dados de uma hora específica.
   */
  def processarHora(spark: SparkSession, fs: FileSystem, hora: LocalDateTime): Unit = {
    // Configuração de conexão JDBC com Oracle
    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admhadoop")
    connectionProperties.put("password", ".admhadoop#")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    val splitByColumn = "NSUDF" // Coluna usada para particionar a importação dos dados
    val numPartitions = 200 // Número de partições para paralelismo

    val (dataInicial, dataFinal) = formatarIntervalo(hora) // Define intervalo de tempo

    val baseQuery = construirQuery(dataInicial, dataFinal) // Constrói a query SQL
    val df = lerDataFrame(spark, jdbcUrl, baseQuery, splitByColumn, numPartitions, connectionProperties, dataInicial, dataFinal) // Lê os dados do banco

    if (df.isDefined) {
      val dfComParticoes = adicionarParticoes(df.get, hora) // Adiciona colunas de partição ao DataFrame
      val targetDir = construirDiretorio(hora) // Define o diretório de destino
      escreverDataFrame(dfComParticoes, fs, targetDir) // Escreve os dados em formato Parquet
    }
  }

  /**
   * Processa uma hora que estava faltando.
   */
  def processarHoraFaltante(spark: SparkSession, fs: FileSystem, horaFaltante: LocalDateTime): Unit = {
    processarHora(spark, fs, horaFaltante)
  }
  /**
   * Formata o intervalo de tempo no padrão necessário para consulta ao Oracle.
   */
  def formatarIntervalo(hora: LocalDateTime): (String, String) = {
    val formatterData = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val formatterHora = DateTimeFormatter.ofPattern("HH")

    val dataFormatada = hora.format(formatterData)
    val horaFormatada = hora.format(formatterHora)

    val dataInicial = s"$dataFormatada $horaFormatada:00:00"
    val dataFinal = s"$dataFormatada $horaFormatada:59:59"

    (dataInicial, dataFinal)
  }
  /**
   * Constrói a query SQL para buscar os dados no banco Oracle.
   */
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
  /**
   * Lê os dados do Oracle e retorna um DataFrame particionado.
   */
  def lerDataFrame(
                    spark: SparkSession,           // Sessão Spark para realizar a leitura dos dados
                    jdbcUrl: String,               // URL de conexão com o banco de dados
                    baseQuery: String,             // Query SQL base para extração dos dados
                    splitByColumn: String,         // Coluna usada para particionar os dados na leitura
                    numPartitions: Int,            // Número de partições desejado para otimizar a leitura
                    connectionProperties: Properties, // Propriedades de conexão JDBC (usuário, senha, etc.)
                    dataInicial: String,           // Data inicial do intervalo de extração
                    dataFinal: String              // Data final do intervalo de extração
                  ): Option[DataFrame] = {
    // Consulta SQL para determinar o valor mínimo e máximo da coluna de particionamento
    val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
    // Lê os valores mínimo e máximo diretamente do banco de dados para definir os limites das partições
    val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)
    // Obtém os valores mínimo e máximo das colunas retornadas pela consulta
    val minRow = minMaxDF.select("min").first()
    val maxRow = minMaxDF.select("max").first()
    // Caso não existam dados no intervalo consultado, retorna None para evitar falhas no processamento
    if (minRow.isNullAt(0) || maxRow.isNullAt(0)) {
      println(s"Não há dados para o intervalo de tempo $dataInicial - $dataFinal. Pulando o processamento.")
      None
    } else {    // Converte os valores mínimo e máximo para Double e depois para Long para evitar problemas de precisão
      val min = minRow.getAs[java.math.BigDecimal](0).doubleValue()
      val max = maxRow.getAs[java.math.BigDecimal](0).doubleValue()
      // a partição para paralelizar a leitura dos dados
      val partitionBounds = (min.toLong to max.toLong by ((max - min) / numPartitions).toLong).toList
      // Realiza a leitura dos dados usando JDBC com particionamento
      val df = spark.read.jdbc(
        jdbcUrl, // URL do banco de dados
        s"($baseQuery) tmp", // Query SQL para extrair os dados
        splitByColumn, // Coluna utilizada para dividir os dados em partições
        partitionBounds.head, // Valor mínimo para a primeira partição
        partitionBounds.last, // Valor máximo para a última partição
        numPartitions, // Número de partições desejado
        connectionProperties // Propriedades de conexão com o banco
      )
      // Retorna o DataFrame encapsulado em Some()
      Some(df)
    }
  }

  /**
   * Adiciona colunas de partição ao DataFrame com base na hora de processamento.
   */
  def adicionarParticoes(df: DataFrame, hora: LocalDateTime): DataFrame = {
    df
      .withColumn("ano", lit(hora.getYear)) // Extrai o ano
      .withColumn("mes", lit(hora.getMonthValue)) // Extrai o mês
      .withColumn("dia", lit(hora.getDayOfMonth)) // Extrai o dia
      .withColumn("hora", lit(hora.getHour)) // Extrai a hora
  }
  /**
   * Constrói o caminho do diretório de destino baseado na hora de processamento.
   */
  def construirDiretorio(hora: LocalDateTime): String = {
    s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/ano=${hora.getYear}/mes=${hora.getMonthValue}/dia=${hora.getDayOfMonth}/hora=${hora.getHour}"
  }
  /**
   * Escreve o DataFrame em formato Parquet no HDFS.
   */
  def escreverDataFrame(df: DataFrame, fs: FileSystem, targetDir: String): Unit = {
    val path = new Path(targetDir)// Cria um objeto Path com o diretório de destino onde o DataFrame será salvo
    // Reparticiona o DataFrame em 4 partições com base nas colunas "ano", "mes", "dia" e "hora".
    // Isso melhora a distribuição dos dados ao gravá-los no HDFS e permite leitura eficiente posteriormente.
    val dfReparticionado = df.repartition(4, col("ano"), col("mes"), col("dia"), col("hora"))
    if (!fs.exists(path)) {  // Verifica se o diretório de destino já existe no HDFS
      // Se o diretório **não existir**, escreve o DataFrame em formato Parquet com compressão LZ4.
      // O formato Parquet é escolhido por ser eficiente em consultas analíticas.
      dfReparticionado.write
        .option("compression", "lz4")
        .parquet(targetDir)
    } else {
      println(s"O diretório $targetDir já existe. Pulando a escrita.")// Caso o diretório já exista, exibe uma mensagem e evita sobrescrever os dados existentes.
    }
  }
  /**
   * Identifica horas que não foram processadas nos últimos dois dias.
   */
  def identificarHorasFaltantes(fs: FileSystem, inicio: LocalDateTime, fim: LocalDateTime): List[LocalDateTime] = {
    val formatterAnoMesDiaHora = DateTimeFormatter.ofPattern("yyyyMMddHH")
    // Lista mutável para armazenar as horas faltantes (para melhor desempenho na adição de elementos)
    val horasFaltantes = scala.collection.mutable.ListBuffer.empty[LocalDateTime]
    var horaAtual = inicio  // Define a hora inicial da verificação
    while (horaAtual.isBefore(fim)) {// Percorre todas as horas dentro do intervalo de tempo
      // Formata a data e hora no padrão esperado (não está sendo usado diretamente, mas pode ser útil para logs)
      val anoMesDiaHora = horaAtual.format(formatterAnoMesDiaHora)
      // Constrói o caminho esperado no HDFS baseado na data/hora atual
      val path = new Path(s"/datalake/bronze/sources/dbms/dec/processamento/nfe/porhora/processar/ano=${horaAtual.getYear}/mes=${horaAtual.getMonthValue}/dia=${horaAtual.getDayOfMonth}/hora=${horaAtual.getHour}")
      // Verifica se o diretório correspondente existe no HDFS
      if (!fs.exists(path)) {
        horasFaltantes += horaAtual // Caso não exista, adiciona essa hora à lista de horas faltantes
      }

      horaAtual = horaAtual.plusHours(1) // Incrementa a hora para verificar a próxima
    }

    horasFaltantes.toList // Retorna a lista final com todas as horas cujos diretórios não foram encontrados no HDFS
  }
}
// extratorNFePorHoraProcessorIncremental.main(Array())