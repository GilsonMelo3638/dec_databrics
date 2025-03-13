package inferenciaSchema


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
// Defina o schema XML para as tags que você deseja extrair


object SchemaNF3e {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfNF3e").enableHiveSupport().getOrCreate()
    import spark.implicits._
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "nf3eProc").load("/datalake/bronze/sources/dbms/dec/nf3e/202307_202501")
    // Agora você pode acessar infNFe

    df.printSchema()
  }
}

//SchemaNF3e.main(Array())
