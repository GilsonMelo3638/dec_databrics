package Sqoop
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.util.Properties

object SqoopNFeProcessorListaCsv {

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("SqoopToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    // Importa as implicits do Spark (necessário para operações como .as[String])
    import spark.implicits._

    // Caminho do arquivo CSV
    val csvPath = "/tmp/problemaitem"

    // Lê o arquivo CSV e extrai as chaves
    val chavesDF = spark.read
      .option("header", "true") // Assume que o arquivo tem cabeçalho
      .csv(csvPath)
      .selectExpr("chave") // Seleciona a coluna de chaves

    // Coleta as chaves como uma lista de strings
    val chaves = chavesDF.select("chave").as[String].collect().toList

    // Configurações de conexão com o banco de dados Oracle
    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admhadoop")
    connectionProperties.put("password", ".admhadoop#")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    // Query SQL base para selecionar os dados da tabela DEC_DFE_NFE
    val baseQuery =
      s"""
    SELECT NSUDF,
           REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nfeProc', 'xmlns=\"http://www.portalfiscal.inf.br/nfe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
           f.NSUAN, f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
           TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
           TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
           TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
           f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
    FROM DEC_DFE_NFE f
    WHERE f.CHAVE IN (${chaves.map(chave => s"'$chave'").mkString(",")})
  """

    // Coluna para particionamento (equivalente ao --split-by do Sqoop)
    val splitByColumn = "NSUDF"

    // Número de partições (equivalente ao --num-mappers do Sqoop)
    val numPartitions = 200

    // Obtém os valores mínimo e máximo da coluna de particionamento
    val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
    val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

    // Converte java.math.BigDecimal para Double
    val min = minMaxDF.select("min").first().getAs[java.math.BigDecimal](0).doubleValue()
    val max = minMaxDF.select("max").first().getAs[java.math.BigDecimal](0).doubleValue()

    // Cria as partições com base no intervalo de valores da coluna de particionamento
    val partitionBounds = (min.toLong to max.toLong by ((max - min) / numPartitions).toLong).toList

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

    // Define o caminho de destino no HDFS
    val targetDir = "/tmp/problemaitem/xml"

    // Verifica se o diretório de destino já existe e o deleta, se necessário
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(targetDir))) {
      fs.delete(new Path(targetDir), true) // true para deletar recursivamente
    }

    // Salva os dados no HDFS no formato Parquet com compressão LZ4
    df.write
      .option("compression", "lz4")
      .parquet(targetDir)
  }
}

//SqoopNFeProcessorListaCsv.main(Array())