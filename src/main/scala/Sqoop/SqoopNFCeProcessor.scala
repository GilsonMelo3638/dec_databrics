//scp "C:\dec\target\DecInfNFePrata-0.0.1-SNAPSHOT.jar"  gamelo@10.69.22.71:src/main/scala/DecInfNFePrata-0.0.1-SNAPSHOT.jar
//hdfs dfs -put -f /export/home/gamelo/src/main/scala/DecInfNFePrata-0.0.1-SNAPSHOT.jar /app/dec
//hdfs dfs -ls /app/dec
// hdfs dfs -rm -skipTrash /app/dec/DecInfNFePrata-0.0.1-SNAPSHOT.jar
// spark-submit \
//  --class DECJob.InfNFeProcessor \
//  --master yarn \
//  --deploy-mode cluster \
//  --num-executors 20 \
//  --executor-memory 4G \
//  --executor-cores 2 \
//  --conf "spark.sql.parquet.writeLegacyFormat=true" \
//  --conf "spark.sql.debug.maxToStringFields=100" \
//  --conf "spark.executor.memoryOverhead=1024" \
//  --conf "spark.network.timeout=800s" \
//  --conf "spark.yarn.executor.memoryOverhead=4096" \
//  --conf "spark.shuffle.service.enabled=true" \
//  --conf "spark.dynamicAllocation.enabled=true" \
//  --conf "spark.dynamicAllocation.minExecutors=10" \
//  --conf "spark.dynamicAllocation.maxExecutors=40" \
//  --packages com.databricks:spark-xml_2.12:0.13.0 \
//  hdfs://sepladbigdata/app/dec/DecInfNFePrata-0.0.1-SNAPSHOT.jar
package Sqoop

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
object SqoopNFCeProcessor {

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("SqoopToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    // Obtém a data de ontem no fuso horário desejado
    val ontem = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(1)

    // Formata as variáveis conforme necessário
    val formatterAnoMes = DateTimeFormatter.ofPattern("yyyyMM")
    val formatterAnoMesDia = DateTimeFormatter.ofPattern("yyyyMMdd")
    val formatterDataHora = DateTimeFormatter.ofPattern("dd/MM/yyyy")

    val anoMes = ontem.format(formatterAnoMes)       // Formato: "202502"
    val anoMesDia = ontem.format(formatterAnoMesDia) // Formato: "20250201"
    val dataFormatada = ontem.format(formatterDataHora)

    val dataInicial = s"$dataFormatada 00:00:00"
    val dataFinal = s"$dataFormatada 23:59:59"

    // Exibe as variáveis
    println(s"anoMes: $anoMes")
    println(s"anoMesDia: $anoMesDia")
    println(s"dataInicial: $dataInicial")
    println(s"dataFinal: $dataFinal")

    // Configurações de conexão com o banco de dados Oracle
    val jdbcUrl = "jdbc:oracle:thin:@sefsrvprd704.fazenda.net:1521/ORAPRD21"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "userdec")
    connectionProperties.put("password", "userdec201811")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    // Coluna para particionamento (equivalente ao --split-by do Sqoop)
    val splitByColumn = "NSU"

    // Número de partições (equivalente ao --num-mappers do Sqoop)
    val numPartitions = 600

    // Query SQL base
    val baseQuery =
      s"""
    SELECT NSU,
           REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nfeProc', 'xmlns=\"http://www.portalfiscal.inf.br/nfe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
           f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
           TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
           TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
           TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
           f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
    FROM DEC_DFE_NFCE f
    WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
  """

    // Obtém os valores mínimo e máximo da coluna de particionamento como java.math.BigDecimal
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
    val targetDir = s"/datalake/bronze/sources/dbms/dec/processamento/nfce/processar/$anoMesDia"

    // Salva os dados no HDFS no formato Parquet com compressão LZ4
    df.write
      .option("compression", "lz4")
      .parquet(targetDir)
 }
}

//SqoopNFCeProcessor.main(Array())