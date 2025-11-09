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
package Utils.Extracoes

import org.apache.spark.sql.SparkSession
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Properties

object testeExtratorGenerico {

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("ExtratorToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    // Coluna para particionamento (equivalente ao --split-by do Sqoop)
    val splitByColumn = "NSU"
    val documento = "nfcom"

    // Configurações de conexão com o banco de dados Oracle
    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admhadoop")
    connectionProperties.put("password", ".admhadoop#")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    // Número de partições (equivalente ao --num-mappers do Sqoop)
    val numPartitions = 200

    // Define o intervalo de dias (de -2 até -4)
    val dias = List(-1)

    // Loop para processar cada dia
    dias.foreach { dia =>
      // Obtém a data correspondente ao dia atual do loop
      val data = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(Math.abs(dia))

      // Formata as variáveis conforme necessário
      val formatterAnoMes = DateTimeFormatter.ofPattern("yyyyMM")
      val formatterAnoMesDia = DateTimeFormatter.ofPattern("yyyyMMdd")
      val formatterDataHora = DateTimeFormatter.ofPattern("dd/MM/yyyy")

      val anoMes = data.format(formatterAnoMes)       // Formato: "202502"
      val anoMesDia = data.format(formatterAnoMesDia) // Formato: "20250201"
      val dataFormatada = data.format(formatterDataHora)

      val dataInicial = s"$dataFormatada 00:00:00"
      val dataFinal = s"$dataFormatada 23:59:59"

      // Exibe as variáveis
      println(s"Processando dia: $dia")
      println(s"anoMes: $anoMes")
      println(s"anoMesDia: $anoMesDia")
      println(s"dataInicial: $dataInicial")
      println(s"dataFinal: $dataFinal")

      // Query SQL base
      val baseQuery =
//        s"""
//SELECT NSU,
//REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nf3eProc', 'xmlns=\"http://www.portalfiscal.inf.br/nf3e\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
//     f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
//     TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
//     TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
//     TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
//     f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
//FROM DEC_DFE_NF3E f
//WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
//"""

//        s"""
//    SELECT NSUSVD,
//COALESCE(
//            REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//cteSimpProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
//            REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//cteProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
//            REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//cteOSProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' '),
//            REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//GTVeProc', 'xmlns=\"http://www.portalfiscal.inf.br/cte\"') AS CLOB), CHR(10), ' '), CHR(13), ' ')
//        ) AS XML_DOCUMENTO_CLOB,
//      f.NSUAUT,
//      f.CSTAT,
//      f.CHAVE,
//      f.IP_TRANSMISSOR,
//      TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
//      TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
//      TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
//      f.EMITENTE,
//      f.UF_EMITENTE,
//      f.DESTINATARIO,
//      f.UF_DESTINATARIO,
//      f.MODELO,
//      f.TPEMIS
//      FROM DEC_DFE_CTE_SVD f
//    WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
//  """

//      s"""
//SELECT NSU,
//REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//nf3eProc', 'xmlns=\"http://www.portalfiscal.inf.br/nf3e\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
//     f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
//     TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
//     TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
//     TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
//     f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
//FROM DEC_DFE_NF3E f
//WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
//"""

//        s"""
//SELECT NSU,
//REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//bpeProc', 'xmlns=\"http://www.portalfiscal.inf.br/bpe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
//     f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
//     TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
//     TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
//     TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
//     f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
//FROM DEC_DFE_BPE f
//WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
//"""

      s"""
        SELECT NSU,
    REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//NFComProc', 'xmlns=\"http://www.portalfiscal.inf.br/nfcom\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
               f.CSTAT, f.CHAVE, f.IP_TRANSMISSOR,
               TO_CHAR(f.DHRECBTO, 'DD/MM/YYYY HH24:MI:SS') AS DHRECBTO,
               TO_CHAR(f.DHEMI, 'DD/MM/YYYY HH24:MI:SS') AS DHEMI,
               TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
               f.EMITENTE, f.UF_EMITENTE, f.DESTINATARIO, f.UF_DESTINATARIO
        FROM DEC_DFE_NFCOM f
        WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
      """

      // Obtém os valores mínimo e máximo da coluna de particionamento como java.math.BigDecimal
      val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
      val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

      // Verifica se há dados para o dia atual
      if (minMaxDF.isEmpty || minMaxDF.head().isNullAt(0) || minMaxDF.head().isNullAt(1)) {
        println(s"Não há dados para o dia $dia. Pulando...")
      } else {
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

        // Define o caminho de destino no HDFS
        val targetDir = s"/datalake/bronze/sources/dbms/dec/processamento/$documento/processar/$anoMesDia"

        // Salva os dados no HDFS no formato Parquet com compressão LZ4
        df.write
          .option("compression", "lz4")
          .parquet(targetDir)

        println(s"Dados do dia $dia salvos em: $targetDir")
      }
    }
  }
}

//testeExtratorGenerico.main(Array())