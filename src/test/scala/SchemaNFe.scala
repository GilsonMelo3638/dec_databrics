
import org.apache.spark.sql.SparkSession

// Defina o schema XML para as tags que você deseja extrair
object SchemaNFe {
def main(args: Array[String]): Unit = {
  // Criação da sessão Spark com suporte ao Hive
  val spark = SparkSession.builder.appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
  // Diretório dos arquivos Parquet
  // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
  val df = spark.read.format("xml").option("rowTag", "nfeProc").load("/datalake/bronze/sources/dbms/dec/nfe/202410")
  // Agora você pode acessar infNFe

  //  df.show(2, truncate = false)
  df.printSchema()

}}

//SchemaNFe.main(Array())