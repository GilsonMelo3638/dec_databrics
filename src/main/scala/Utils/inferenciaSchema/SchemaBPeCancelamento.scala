package inferenciaSchema


import org.apache.spark.sql.SparkSession

// Defina o schema XML para as tags que você deseja extrair
object SchemaBPeCancelamento {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfBPeCancelamento").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "procEventoBPe").load("/datalake/bronze/sources/dbms/dec/bpe_cancelamento/201909_202502")
    // Agora você pode acessar infNFe

    //  df.show(2, truncate = false)
    df.printSchema()

  }}

//SchemaBPeCancelamento.main(Array())