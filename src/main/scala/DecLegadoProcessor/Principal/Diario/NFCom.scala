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
package DecLegadoProcessor.Principal.Diario

import Processors.NFComProcessor
import Schemas.NFComSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object NFCom {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val ano = 2025
  val mesInicio = 1
  val mesFim = 1
  val tipoDocumento = "nfcom"  // ÚNICO lugar onde o tipo é definido
  val diretorioProcessar = "20260327"

  // Função para formatar o nome do documento conforme necessário para cada contexto
  def formatoBronze(tipo: String): String = {
    // Para NF3e, queremos "NF3e" (N e F maiúsculos, e minúsculo)
    tipo match {
      case "nf3e" => "NF3e"
      case "nfce" => "NFCe"
      case "nfcom" => "NFCom"
      case _ => tipo.toUpperCase
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInNF3e").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe NFComSchema
    val schema = NFComSchema.createSchema()

    // Lista de meses com base nas variáveis externas
    val anoMesList = (mesInicio to mesFim).map { month =>
      f"$ano${month}%02d"
    }.toList

    anoMesList.foreach { anoMes =>
      // CAMINHO BRONZE - usando a função de formatação
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/processamento/${formatoBronze(tipoDocumento)}/processar/$diretorioProcessar"

      // Registrar o horário de início da iteração
      val startTime = LocalDateTime.now()
      println(s"Início da iteração para $ano: $startTime")
      println(s"Lendo dados do caminho: $parquetPath")

      // 1. Carrega o arquivo Parquet
      val parquetDF = spark.read.parquet(parquetPath)

      // 2. Seleciona as colunas e filtra MODELO = 64
      val xmlDF = parquetDF
        .select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSU".cast("string").as("NSU"),
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR"
        )
      xmlDF.show()

      // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
      val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))

      // 4. Gera o DataFrame selectedDF
      implicit val sparkSession: SparkSession = spark
      val selectedDF = NFComProcessor.generateSelectedDF(parsedDF)
      val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

      // Imprimir no console as variações e a contagem de 'chave_particao'
      val chaveParticaoContagem = selectedDFComParticao
        .groupBy("chave_particao")
        .agg(count("chave").alias("contagem_chaves"))
        .orderBy("chave_particao")

      chaveParticaoContagem.collect().foreach { row =>
        println(s"Variação: ${row.getAs[String]("chave_particao")}, Contagem: ${row.getAs[Long]("contagem_chaves")}")
      }

      // Redistribuir os dados
      val repartitionedDF = selectedDFComParticao.repartition(1)

      // CAMINHO PRATA - usando tipoDocumento original (minúsculo) e formatoBronze para a pasta final
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024)
        .partitionBy("chave_particao")
        .save(s"/datalake/prata/sources/dbms/dec/${tipoDocumento}/${formatoBronze(tipoDocumento)}")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")

      // CAMINHOS ORIGEM E DESTINO BRONZE - usando a função de formatação
      val origemPath = new Path(s"/datalake/bronze/sources/dbms/dec/processamento/${formatoBronze(tipoDocumento)}/processar/$diretorioProcessar")
      val destinoPath = new Path(s"/datalake/bronze/sources/dbms/dec/processamento/${formatoBronze(tipoDocumento)}/processado/$diretorioProcessar")

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      println(s"Movendo diretório de $origemPath para $destinoPath")

      if (fs.exists(destinoPath)) {
        println(s"Destino já existe. Apagando: $destinoPath")
        fs.delete(destinoPath, true)
      }

      val moveOk = fs.rename(origemPath, destinoPath)

      if (moveOk) {
        println(s"Movimentação concluída com sucesso.")
      } else {
        println(s"Falha ao mover diretório!")
      }
    }
  }
}
//NF3e.main(Array())

