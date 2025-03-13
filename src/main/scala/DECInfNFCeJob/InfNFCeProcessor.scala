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
package DECInfNFCeJob

import Processors.NFCeProcessor
import Schemas.NFCeSchema

import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object InfNFCeProcessor {
  def main(args: Array[String]): Unit = {
    val tipoDocumento = "nfce"

    val spark = SparkSession.builder()
      .appName("ExtractInfNFe")
      .config("spark.sql.broadcastTimeout", "600") // Timeout para operações de broadcast
      .config("spark.executor.memory", "16g") // Memória do executor
      .config("spark.driver.memory", "8g") // Memória do driver
      .config("spark.executor.memoryOverhead", "4096") // Overhead de memória do executor
      .config("spark.yarn.executor.memoryOverhead", "4096") // Overhead de memória no YARN
      .config("spark.network.timeout", "600s") // Tempo de timeout da rede
      .config("spark.executor.heartbeatInterval", "30s") // Evita perda de executores
      .config("spark.sql.autoBroadcastJoinThreshold", "-1") // Desabilita broadcast automático
      .config("spark.sql.shuffle.partitions", "800") // Aumenta partições no shuffle
      .config("spark.default.parallelism", "800") // Melhora paralelismo
      .config("spark.shuffle.service.enabled", "true") // Ativa serviço de shuffle
      .config("spark.shuffle.file.buffer", "1m") // Buffer maior para reduzir I/O
      .config("spark.reducer.maxSizeInFlight", "96m") // Reduz pressão no shuffle
      .config("spark.memory.fraction", "0.6") // Ajusta a fração de memória para execução
      .config("spark.memory.storageFraction", "0.5") // Ajusta fração de memória para armazenamento
      .config("spark.dynamicAllocation.enabled", "true") // Ativa alocação dinâmica
      .config("spark.dynamicAllocation.minExecutors", "10") // Mínimo de executores
      .config("spark.dynamicAllocation.maxExecutors", "40") // Máximo de executores
      .config("spark.dynamicAllocation.initialExecutors", "20") // Melhor distribuição inicial
      .config("spark.sql.hive.filesourcePartitionFileCacheSize", "524288000") // Cache de partições Hive
      .config("spark.storage.replication", "2") // Mantém duas réplicas
      .enableHiveSupport() // Ativa suporte ao Hive
      .getOrCreate()

    import spark.implicits._

    // Definindo intervalo de dias: diasAntesInicio (10 dias atrás) até diasAntesFim (ontem)
    val diasAntesInicio = LocalDate.now.minusDays(15)
    val diasAntesFim = LocalDate.now.minusDays(1)

    // Formatação para ano, mês e dia
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

      // Verificar se o diretório existe antes de processar
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val parquetPathExists = fs.exists(new Path(parquetPath))

      val parquetPathContentExists = fs.exists(new Path(parquetPath))

      if (parquetPathContentExists) {
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
          // 2. Selecionar a coluna que contém o XML (ex: "XML_DOCUMENTO_CLOB")
          val xmlDF = parquetDF.select(
            $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
            $"NSU".cast("string").as("NSU"),
            $"DHPROC",
            $"DHEMI",
            $"IP_TRANSMISSOR"
          )
          // 3. Usar `from_xml` para ler o XML da coluna usando o esquema definido
          val schema = NFCeSchema.createSchema()
          val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))

          // 4. Selecionar os campos desejados
          implicit val sparkSession: SparkSession = spark // Passando o SparkSession implicitamente
          val selectedDF = NFCeProcessor.generateSelectedDF(parsedDF) // Criando uma nova coluna 'chave_particao' extraindo os dígitos 3 a 6 da coluna 'CHAVE'
          val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

          // Obtendo as variações únicas de 'chave_particao' e coletando para uma lista
          val chaveParticoesUnicas = selectedDFComParticao
            .select("chave_particao")
            .distinct()
            .as[String]
            .collect()

          // Criando uma função para processar cada partição separadamente
          def processarParticao(chaveParticao: String): Unit = {
            val caminhoParticao = s"$destino/chave_particao=$chaveParticao"

            val particaoExiste = try {
              val particaoDF = spark.read.parquet(caminhoParticao).select("chave")
              !particaoDF.isEmpty
            } catch {
              case _: Exception => false
            }

            val dfFiltrado = if (particaoExiste) {
              println(s"[INFO] Partição $chaveParticao já existe. Filtrando novas chaves...")
              val particaoDF = spark.read.parquet(caminhoParticao).select("chave").distinct()
              selectedDFComParticao
                .filter(col("chave_particao") === chaveParticao)
                .join(particaoDF, Seq("chave"), "left_anti")
            } else {
              println(s"[INFO] Partição $chaveParticao não existe. Criando nova partição...")
              selectedDFComParticao.filter(col("chave_particao") === chaveParticao)
            }

            //            val qtdRegistros = dfFiltrado.count()
            //            println(s"[INFO] Partição $chaveParticao - Quantidade de registros a serem salvos: $qtdRegistros")

            dfFiltrado.write
              .mode("append")
              .format("parquet")
              .option("compression", "lz4")
              .option("parquet.block.size", 500 * 1024 * 1024)
              .save(caminhoParticao)

            println(s"[INFO] Finalizado processamento da partição: $chaveParticao")
          }

          // Iterando sobre cada partição e processando
          chaveParticoesUnicas.foreach(processarParticao)

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
          println(s"Diretório de origem $parquetPath não encontrado.")
        }
      }
    }
  }
}
//InfNFCeProcessor.main(Array())
