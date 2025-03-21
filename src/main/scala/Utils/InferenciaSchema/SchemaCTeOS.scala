package Utils.InferenciaSchema

import com.databricks.spark.xml._
import org.apache.spark.sql.{DataFrame, SparkSession}


object SchemaCTeOS {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    // Criar sessão Spark otimizada
    val spark = SparkSession.builder()
      .appName("ExtractInfNFe")
      .config("spark.sql.parquet.writeLegacyFormat", "true") // Evita problemas de compatibilidade
      .config("spark.executor.memory", "8g") // Aumenta memória do executor
      .config("spark.executor.cores", "2") // Limita núcleos por executor
      .config("spark.driver.memory", "4g") // Aumenta memória do driver
      .config("spark.network.timeout", "600s") // Timeout para evitar desconexões
      .config("spark.sql.shuffle.partitions", "200") // Otimiza o número de partições
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Carregar o DataFrame do Parquet
    val parquetDF: DataFrame = spark.read.parquet("/datalake/bronze/sources/dbms/dec/cte/2024")
    // Filtrar a coluna 'modelo' para incluir apenas registros onde modelo == 67
    val filteredDF: DataFrame = parquetDF.filter($"modelo" === 67)
    // Selecionar apenas a coluna 'XML_DOCUMENTO_CLOB' e converter para Dataset[String]
    val xmlDF = filteredDF.select($"XML_DOCUMENTO_CLOB".as[String]).cache()
    // Exibir os primeiros registros (XML completo)
    xmlDF.show(truncate = false)
    // Inferir o schema do XML
    val inferredSchema = spark.read.option("rowTag", "cteOSProc").xml(xmlDF)
    xmlDF.printSchema()
  }
}
//SchemaCTeOS.main(Array())