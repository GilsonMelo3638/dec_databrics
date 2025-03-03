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

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Properties

object SqoopProcessor {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: SqoopProcessor <documentType> <splitByColumn> <targetDir>")
      System.exit(1)
    }

    val documentType = args(0) // BPe, CTe, MDFe, NF3e
    val splitByColumn = args(1) // Coluna de particionamento
    val targetDirBase = args(2) // Caminho base de destino no HDFS

    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName(s"SqoopToSparkWithPartitioning-$documentType")
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

    // Número de partições (equivalente ao --num-mappers do Sqoop)
    val numPartitions = 50

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
        f.CSTAT,
        f.CHAVE,
        f.IP_TRANSMISSOR,
        TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
        TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
        TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
        f.EMITENTE,
        f.UF_EMITENTE,
        f.DESTINATARIO,
        f.UF_DESTINATARIO
        FROM DEC_DFE_NF3E f
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
    val targetDir = s"$targetDirBase/$documentType/processar/$anoMesDia"

    // Salva os dados no HDFS no formato Parquet com compressão LZ4
    df.write
      .option("compression", "lz4")
      .parquet(targetDir)
  }
}