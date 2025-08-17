package Utils.InferenciaSchema

import org.apache.spark.sql.SparkSession

// Defina o schema XML para as tags que você deseja extrair
object SchemaNFComCancelamento {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfNFComCancelamento").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "procEventoNFCom").load("/datalake/bronze/sources/dbms/dec/nfcom_cancelamento/2025")
    // Agora você pode acessar infNFe

    //  df.show(2, truncate = false)
    df.printSchema()

  }}

//SchemaNFComCancelamento.main(Array())