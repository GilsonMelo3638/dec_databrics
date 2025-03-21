package Utils.InferenciaSchema

import com.databricks.spark.xml._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SchemaCTeSimpProc {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    import spark.implicits._

    // Carregar o DataFrame do Parquet
    val parquetDF: DataFrame = spark.read.parquet("/datalake/bronze/sources/dbms/dec/processamento/cte/processar/20250319")
    // Filtrar a coluna 'modelo' para incluir apenas registros onde modelo == 57
    val filteredDF: DataFrame = parquetDF
      .filter($"modelo" === 57) // Filtra onde MODELO é igual a 57
      .filter($"XML_DOCUMENTO_CLOB".rlike("<cteSimpProc")) // Filtra onde XML_DOCUMENTO_CLOB contém <cteSimpProc>
    // Selecionar apenas a coluna 'XML_DOCUMENTO_CLOB' e converter para Dataset[String]
    val xmlDF = filteredDF.select($"XML_DOCUMENTO_CLOB".as[String]).cache()

    // Exibir os primeiros registros (XML completo)
    xmlDF.show(truncate = false)

    // Inferir o schema do XML
    val inferredSchema = spark.read
      .option("rowTag", "cteSimpProc") // Tag raiz do XML
      .xml(xmlDF)

    // Exibir o schema inferido
    inferredSchema.printSchema()
    //  df.show(2, truncate = false)
    xmlDF.printSchema()
  }
}


//SchemaCTeSimpProc.main(Array())