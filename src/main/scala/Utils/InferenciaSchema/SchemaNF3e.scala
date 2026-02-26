package Utils.InferenciaSchema

import org.apache.spark.sql.SparkSession
// Defina o schema XML para as tags que você deseja extrair


object SchemaNF3e {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfNF3e").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "nf3eProc").load("/datalake/bronze/sources/dbms/dec/diario/nf3e/year=2026/month=02/day=23/part-00020-83a767c6-4142-4c5b-afdc-a2b9bf93eb91.c000.lz4.parquet")
    // Agora você pode acessar infNFe

    df.printSchema()
  }
}

//SchemaNF3e.main(Array())
