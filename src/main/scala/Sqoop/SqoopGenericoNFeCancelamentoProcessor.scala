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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Properties

object SqoopGenericoNFeCancelamentoProcessor {

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("SqoopToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    // Loop para gerar intervalos de minusDays de -1 a -15
    for (daysAgo <- 1 to 15) {
      // Obtém a data correspondente ao número de dias atrás no fuso horário desejado
      val data = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(daysAgo)

      // Formata as variáveis conforme necessário
      val formatterAnoMes = DateTimeFormatter.ofPattern("yyyyMM")
      val formatterAnoMesDia = DateTimeFormatter.ofPattern("yyyyMMdd")
      val formatterDataHora = DateTimeFormatter.ofPattern("dd/MM/yyyy")

      val anoMes = data.format(formatterAnoMes) // Formato: "202502"
      val anoMesDia = data.format(formatterAnoMesDia) // Formato: "20250201"
      val dataFormatada = data.format(formatterDataHora)

      val dataInicial = s"$dataFormatada 00:00:00"
      val dataFinal = s"$dataFormatada 23:59:59"

      // Exibe as variáveis
      println(s"Processando data: $dataFormatada")
      println(s"anoMes: $anoMes")
      println(s"anoMesDia: $anoMesDia")
      println(s"dataInicial: $dataInicial")
      println(s"dataFinal: $dataFinal")

      // Caminhos de destino no HDFS
      val targetDirProcessar = s"/datalake/bronze/sources/dbms/dec/processamento/nfe_cancelamento/processar/$anoMesDia"
      val targetDirProcessado = s"/datalake/bronze/sources/dbms/dec/processamento/nfe_cancelamento/processado/$anoMesDia"

      // Verifica se o arquivo já existe em processar ou processado
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(targetDirProcessar)) || fs.exists(new Path(targetDirProcessado))) {
        println(s"Arquivo já existe em $targetDirProcessar ou $targetDirProcessado. Processamento interrompido para a data $dataFormatada.")
      } else {
        // Configurações de conexão com o banco de dados Oracle
        val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
        val connectionProperties = new Properties()
        connectionProperties.put("user", "admhadoop")
        connectionProperties.put("password", ".admhadoop#")
        connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

        // Coluna para particionamento (equivalente ao --split-by do Sqoop)
        val splitByColumn = "NSUDF"

        // Número de partições (equivalente ao --num-mappers do Sqoop)
        val numPartitions = 50

        // Query SQL base
        val baseQuery =
          s"""
          SELECT
            NSUDF,
            REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//procEventoNFe', 'xmlns=\"http://www.portalfiscal.inf.br/nfe\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
            f.NSUAN,
            f.CSTAT,
            f.CHAVE,
            f.IP_TRANSMISSOR,
            TO_CHAR(f.DHEVENTO, 'DD/MM/YYYY HH24:MI:SS') AS DHEVENTO,
            TO_CHAR(f.DH_REG_EVENTO, 'DD/MM/YYYY HH24:MI:SS') AS DH_REG_EVENTO,
            TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
            f.SEQ_EVENTO,
            f.TP_EVENTO
          FROM DEC_DFE_NFE_CANCELAMENTO f
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

        // Salva os dados no HDFS no formato Parquet com compressão LZ4
        df.repartition(2)
          .write
          .option("compression", "lz4")
          .option("parquet.block.size", "536870912") // 512 MB
          .parquet(targetDirProcessar)

        println(s"Processamento concluído para a data $dataFormatada.")
      }
    }
  }
}

//SqoopGenericoNFeCancelamentoProcessor.main(Array())