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
package DecDiarioProcessor.Principal

import Processors.NFCeProcessor
import Schemas.NFCeSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object InfNFCe {
  def main(args: Array[String]): Unit = {
    val tipoDocumento = "nfce"

    val spark = SparkSession.builder()
      .appName("ExtractInfNFe")
      .config("spark.sql.broadcastTimeout", "600")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memoryOverhead", "4096")
      .config("spark.yarn.executor.memoryOverhead", "4096")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "30s")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "800")
      .config("spark.default.parallelism", "800")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.shuffle.file.buffer", "1m")
      .config("spark.reducer.maxSizeInFlight", "96m")
      .config("spark.memory.fraction", "0.6")
      .config("spark.memory.storageFraction", "0.5")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.minExecutors", "10")
      .config("spark.dynamicAllocation.maxExecutors", "40")
      .config("spark.dynamicAllocation.initialExecutors", "20")
      .config("spark.sql.hive.filesourcePartitionFileCacheSize", "524288000")
      .config("spark.storage.replication", "2")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Definindo intervalo de dias
    val diasAntesInicio = LocalDate.now.minusDays(15)
    val diasAntesFim = LocalDate.now.minusDays(1)
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    // Iterando pelas datas no intervalo
    (0 to diasAntesInicio.until(diasAntesFim).getDays).foreach { dayOffset =>
      val currentDate = diasAntesInicio.plusDays(dayOffset)

      val ano = currentDate.getYear
      val mes = f"${currentDate.getMonthValue}%02d"
      val dia = f"${currentDate.getDayOfMonth}%02d"
      val anoMesDia = s"$ano$mes$dia"

      val parquetPath = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar/$anoMesDia"
      val parquetPathProcessado = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_det/$anoMesDia"
      val destino = s"/datalake/prata/sources/dbms/dec/$tipoDocumento/infNFCe/"

      println(s"Processando para: Ano: $ano, Mês: $mes, Dia: $dia")
      println(s"Caminho de origem: $parquetPath")
      println(s"Caminho de destino: $parquetPathProcessado")

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val parquetPathExists = fs.exists(new Path(parquetPath))

      if (parquetPathExists) {
        val parquetPathContent = fs.listStatus(new Path(parquetPath))
        val parquetFiles = parquetPathContent.filter(_.getPath.getName.endsWith(".parquet"))

        if (parquetFiles.nonEmpty) {
          val parquetDF = spark.read.parquet(parquetFiles.map(_.getPath.toString): _*)

          // Verificação de quantidade total e distinta
          val totalCount = parquetDF.count()
          val distinctCount = parquetDF.select("chave").distinct().count()

          if (totalCount != distinctCount) {
            println(s"Erro: Total de registros ($totalCount) é diferente do total de registros distintos ($distinctCount) no caminho: $parquetPath")
            throw new IllegalStateException("Inconsistência nos dados: total e distinto não coincidem.")
          } else {
            println(s"Verificação bem-sucedida: Total ($totalCount) e distintos ($distinctCount) são iguais no caminho: $parquetPath")
          }

          val xmlDF = parquetDF.select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            $"NSU".cast("string").as("NSU"),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR"
          )

          val schema = NFCeSchema.createSchema()
          val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))

          implicit val sparkSession: SparkSession = spark
          val selectedDF = NFCeProcessor.generateSelectedDF(parsedDF)
          val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

          // Particionar e processar sem coletar
          selectedDFComParticao
            .write
            .partitionBy("chave_particao")
            .mode("append")
            .format("parquet")
            .option("compression", "lz4")
            .option("parquet.block.size", 500 * 1024 * 1024)
            .save(destino)

          println(s"Gravação concluída para $anoMesDia")

          // Mover os arquivos para a pasta processada
          val srcPath = new Path(parquetPath)
          if (fs.exists(srcPath)) {
            val destPath = new Path(parquetPathProcessado)
            if (!fs.exists(destPath)) {
              fs.mkdirs(destPath)
            }
            fs.listStatus(srcPath).foreach { fileStatus =>
              val srcFile = fileStatus.getPath
              val destFile = new Path(destPath, srcFile.getName)
              fs.rename(srcFile, destFile)
            }
            fs.delete(srcPath, true)
            println(s"Arquivos movidos de $parquetPath para $parquetPathProcessado com sucesso.")
          }
        } else {
          println(s"Nenhum arquivo Parquet encontrado em $parquetPath")
        }
      } else {
        println(s"Diretório de origem $parquetPath não encontrado.")
      }
    }
  }
}
//InfNFCeProcessor.main(Array())
