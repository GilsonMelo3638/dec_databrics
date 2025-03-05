
package DECCTeProcessor
import Processors.CTeProcessor
import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Schemas.CTeSchema

import java.time.LocalDateTime

object CTeLegadoProcessorFaltantes {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val anoInicio = 2020
  val anoFim = 2020
  val tipoDocumento = "cte"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractCTe").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe CTeOSSchema
    val schema = CTeSchema.createSchema()
    // Lista de anos com base nas variáveis externas
    val anoList = (anoInicio to anoFim).map(_.toString).toList

    anoList.foreach { ano =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/processamento/cte/faltantes/"

      // Registrar o horário de início da iteração
      val startTime = LocalDateTime.now()
      println(s"Início da iteração para $ano: $startTime")
      println(s"Lendo dados do caminho: $parquetPath")

      // 1. Carrega o arquivo Parquet
      val parquetDF = spark.read.parquet(parquetPath)

      // 2. Seleciona as colunas e filtra MODELO = 57
      val xmlDF = parquetDF
        .filter($"MODELO" === 57) // Filtra onde MODELO é igual a 64
        .select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSUSVD".cast("string").as("NSUSVD"),
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR",
          $"MODELO".cast("string").as("MODELO"),
          $"TPEMIS".cast("string").as("TPEMIS")
        )
      xmlDF.show()
      // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
      val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))
      //     parsedDF.printSchema()

      // 4. Gera o DataFrame selectedDF usando a nova classe
      implicit val sparkSession: SparkSession = spark // Passando o SparkSession implicitamente
      val selectedDF = CTeProcessor.generateSelectedDF(parsedDF) // Criando uma nova coluna 'chave_particao' extraindo os dígitos 3 a 6 da coluna 'CHAVE'
      val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

      // Imprimir no console as variações e a contagem de 'chave_particao'
      val chaveParticaoContagem = selectedDFComParticao
        .groupBy("chave_particao")
        .agg(count("chave").alias("contagem_chaves"))
        .orderBy("chave_particao")

      // Coletar os dados para exibição no console
      chaveParticaoContagem.collect().foreach { row =>
        println(s"Variação: ${row.getAs[String]("chave_particao")}, Contagem: ${row.getAs[Long]("contagem_chaves")}")
      }

      // Redistribuir os dados para 40 partições
      val repartitionedDF = selectedDFComParticao.repartition(40)

      // Escrever os dados particionados
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
        .partitionBy("chave_particao") // Garante a separação por partição
        .save("/datalake/prata/sources/dbms/dec/cte/CTe")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")
    }
  }
}

//CTeLegadoProcessorFaltantes.main(Array())
