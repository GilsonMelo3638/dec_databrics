package DecDiarioProcessor.Principal

import Processors.NF3eProcessor
import Schemas.NF3eSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object NF3e {
  // Variáveis externas para o tipo de documento
  val tipoDocumento = "nf3e"
  val prataDocumento = "NF3e"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractDiarioNF3e").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe BPeSchema
    val schema = NF3eSchema.createSchema()

    // Gerar a lista dos últimos 10 dias no formato YYYYMMDD, começando do dia anterior
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dataAtual = LocalDateTime.now()
    val ultimos10Dias = (1 to 30).map { diasAtras =>
      dataAtual.minus(diasAtras, ChronoUnit.DAYS).format(dateFormatter)
    }.toList

    // Obter o FileSystem do Hadoop para verificar a existência dos diretórios
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    ultimos10Dias.foreach { dia =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/processamento/$prataDocumento/processar/$dia"
      val parquetPrataPath = s"/datalake/prata/sources/dbms/dec/$tipoDocumento/$prataDocumento"
      val parquetPathProcessado = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processado/$dia"

      // Verificar se o diretório existe
      if (fs.exists(new Path(parquetPath))) {
        // Registrar o horário de início da iteração
        val startTime = LocalDateTime.now()
        println(s"Início da iteração para o dia $dia: $startTime")
        println(s"Lendo dados do caminho: $parquetPath")
        println(s"Caminho de gravação dos dados: $parquetPrataPath")

        try {
          // 1. Carrega o arquivo Parquet
          val parquetDF = spark.read.parquet(parquetPath)

          // Verificação de consistência entre total de registros e registros distintos
          val totalCount = parquetDF.count()
          val distinctCount = parquetDF.distinct().count()

          if (totalCount != distinctCount) {
            println(s"Erro: Total de registros ($totalCount) é diferente do total de registros distintos ($distinctCount) no caminho: $parquetPath")
            throw new IllegalStateException("Inconsistência nos dados: total e distinto não coincidem.")
          } else {
            println(s"Verificação bem-sucedida: Total ($totalCount) e distintos ($distinctCount) são iguais no caminho: $parquetPath")
          }

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

          // 4. Gera o DataFrame selectedDF usando a nova classe
          implicit val sparkSession: SparkSession = spark // Passando o SparkSession implicitamente
          val selectedDF = NF3eProcessor.generateSelectedDF(parsedDF) // Criando uma nova coluna 'chave_particao' extraindo os dígitos 3 a 6 da coluna 'CHAVE'
          val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

//          // Imprimir no console as variações e a contagem de 'chave_particao'
//          val chaveParticaoContagem = selectedDFComParticao
//            .groupBy("chave_particao")
//            .agg(count("chave").alias("contagem_chaves"))
//            .orderBy("chave_particao")
//
//          // Coletar os dados para exibição no console
//          chaveParticaoContagem.collect().foreach { row =>
//            println(s"Variação: ${row.getAs[String]("chave_particao")}, Contagem: ${row.getAs[Long]("contagem_chaves")}")
//          }

          // Redistribuir os dados para 40 partições
          val repartitionedDF = selectedDFComParticao.repartition(1)

          // Escrever os dados particionados
          repartitionedDF
            .write.mode("append")
            .format("parquet")
            .option("compression", "lz4")
            .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
            .partitionBy("chave_particao") // Garante a separação por partição
            .save(parquetPrataPath)

          println(s"Gravação concluída para $dia")

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

          // Registrar o horário de término da gravação
          val saveEndTime = LocalDateTime.now()
          println(s"Gravação concluída: $saveEndTime")
        } catch {
          case e: Exception =>
            println(s"Erro ao processar o dia $dia: ${e.getMessage}")
        }
      } else {
        println(s"Diretório não encontrado para o dia $dia: $parquetPath")
      }
    }
  }
}

//NF3e.main(Array())