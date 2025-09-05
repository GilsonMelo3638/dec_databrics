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
package Extrator

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Properties

object diarioGenericoCancelamento {

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("ExtratorToSparkWithPartitioning")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    // Lista de configurações para cada tipo de documento
    val configs = List(
      ("procEventoNF3e", "nf3e", "NF3E", "nf3e_cancelamento", "NSU"),
      ("procEventoNFe", "nfe", "NFE", "nfe_cancelamento", "NSUDF"),
      ("procEventoNFe", "nfe", "NFCE", "nfce_cancelamento", "NSU"),
      ("procEventoBPe", "bpe", "BPE", "bpe_cancelamento", "NSU"),
      ("procEventoMDFe", "mdfe", "MDFE", "mdfe_cancelamento", "NSU"),
      ("procEventoNFCom", "nfcom", "NFCOM", "nfcom_cancelamento", "NSU"),
      ("procEventoCTe", "cte", "CTE_SVD", "cte_cancelamento", "NSUSVD")
    )

    // Loop para processar cada configuração
    for ((tabAbertura, urlDocumento, tabela, dirBronze, colSplit) <- configs) {
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
        val targetDirProcessar = s"/datalake/bronze/sources/dbms/dec/processamento/$dirBronze/processar/$anoMesDia"
        val targetDirProcessado = s"/datalake/bronze/sources/dbms/dec/processamento/$dirBronze/processado/$anoMesDia"

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
          val splitByColumn = colSplit

          // Número de partições (equivalente ao --num-mappers do Sqoop)
          val numPartitions = 10

          // Query SQL base
          val baseQuery =
            s"""
            SELECT
              ${colSplit},
              REPLACE(REPLACE(XMLSERIALIZE(document f.XML_DOCUMENTO.extract('//$tabAbertura', 'xmlns=\"http://www.portalfiscal.inf.br/$urlDocumento\"') AS CLOB), CHR(10), ' '), CHR(13), ' ') AS XML_DOCUMENTO_CLOB,
              f.CSTAT,
              f.CHAVE,
              f.IP_TRANSMISSOR,
              TO_CHAR(f.DHEVENTO, 'DD/MM/YYYY HH24:MI:SS') AS DHEVENTO,
              TO_CHAR(f.DH_REG_EVENTO, 'DD/MM/YYYY HH24:MI:SS') AS DH_REG_EVENTO,
              TO_CHAR(f.DHPROC, 'DD/MM/YYYY HH24:MI:SS') AS DHPROC,
              f.SEQ_EVENTO,
              f.TP_EVENTO
            FROM DEC_DFE_${tabela}_CANCELAMENTO f
            WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
          """

          // Obtém os valores mínimo e máximo da coluna de particionamento como java.math.BigDecimal
          val minMaxQuery = s"SELECT MIN($splitByColumn) AS min, MAX($splitByColumn) AS max FROM ($baseQuery)"
          val minMaxDF = spark.read.jdbc(jdbcUrl, s"($minMaxQuery) tmp", connectionProperties)

          // Verifica se há dados disponíveis e se os valores de min e max não são nulos
          if (minMaxDF.isEmpty) {
            println(s"Não há dados para a data $dataFormatada. Processamento interrompido.")
          } else {
            val minRow = minMaxDF.select("min").first()
            val maxRow = minMaxDF.select("max").first()

            if (minRow.isNullAt(0) || maxRow.isNullAt(0)) {
              println(s"Valores de min ou max são nulos para a data $dataFormatada. Processamento interrompido.")
            } else {
              // Converte java.math.BigDecimal para Double
              val min = minRow.getAs[java.math.BigDecimal](0).doubleValue()
              val max = maxRow.getAs[java.math.BigDecimal](0).doubleValue()

              // Verifica se min e max são iguais
              if (min == max) {
                // Se min e max são iguais, cria uma única partição
                val df = spark.read.jdbc(
                  jdbcUrl,
                  s"($baseQuery) tmp",
                  splitByColumn, // Coluna de particionamento
                  min.toLong, // Valor mínimo
                  max.toLong, // Valor máximo
                  1, // Número de partições
                  connectionProperties
                )

                // Salva os dados no HDFS no formato Parquet com compressão LZ4
                df.repartition(2)
                  .write
                  .option("compression", "lz4")
                  .option("parquet.block.size", "536870912") // 512 MB
                  .parquet(targetDirProcessar)

                println(s"Processamento concluído para a data $dataFormatada.")
              } else {
                // Calcula o passo (step) e garante que seja pelo menos 1
                val step = Math.max(1, ((max - min) / numPartitions).toLong)

                // Cria as partições com base no intervalo de valores da coluna de particionamento
                val partitionBounds = (min.toLong to max.toLong by step).toList

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
      }
    }
  }
}
//diarioGenericoCancelamento.main(Array())