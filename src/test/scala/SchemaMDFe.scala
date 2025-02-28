
import org.apache.spark.sql.SparkSession
// Defina o schema XML para as tags que você deseja extrair


object SchemaMDFe {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfBPe").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "bpeProc").load("/datalake/bronze/sources/dbms/dec/mdfe/201812_202501")
    // Agora você pode acessar infNFe

    df.printSchema()
  }
}


//SchemaMDFe.main(Array())

