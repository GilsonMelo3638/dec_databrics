package Utils.InferenciaSchema

import org.apache.spark.sql.SparkSession
// Defina o schema XML para as tags que você deseja extrair


object SchemaMDFeCancelamento {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfBPe").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "procEventoMDFe").load("/datalake/bronze/sources/dbms/dec/mdfe_cancelamento/201901_202502")
    // Agora você pode acessar infNFe

    df.printSchema()
  }
}


//SchemaMDFeCancelamento.main(Array())
