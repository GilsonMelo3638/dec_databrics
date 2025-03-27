package Utils.InferenciaSchema

import com.databricks.spark.xml._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SchemaCTeCancelamento {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfCTeCancelamento").enableHiveSupport().getOrCreate()
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "procEventoCTe").load("/datalake/bronze/sources/dbms/dec/cte_cancelamento/201912_202502")
    // Agora você pode acessar infNFe

    df.printSchema()
  }
}


//SchemaCTeCancelamento.main(Array())