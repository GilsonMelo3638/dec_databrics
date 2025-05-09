package Utils.InferenciaSchema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
// Defina o schema XML para as tags que você deseja extrair


object SchemaNFCe {
  val schema = new StructType()
    .add("infNFe", new StructType()
      .add("ide", StringType)
      .add("emit", StringType)
      .add("dest", StringType)
      .add("det", StringType)
      .add("total", StringType)
      .add("transp", StringType)
      .add("cobr", StringType)
      .add("pag", StringType)
      .add("infAdic", StringType)
    )
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    val spark = SparkSession.builder.appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
    import spark.implicits._
    // Diretório dos arquivos Parquet
    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
    val df = spark.read.format("xml").option("rowTag", "nfeProc").load("/datalake/bronze/sources/dbms/dec/nfce/202410")
    // Agora você pode acessar infNFe
    df.select(
      $"NFe.infNFe._Id".as("id"),
      $"protNFe.infProt.chNFe".as("chNFe"),
      $"protNFe.infProt.cStat",
      $"protNFe.infProt.dhRecbto",
      $"NFe.infNFe.ide.dhEmi",
      $"NFe.infNFe.dest.CNPJ".as("cnpj_destinatario"),
      $"NFe.infNFe.dest.CPF".as("cpf_destinatario"),
      $"NFe.infNFe.emit.CNPJ".as("cnpj_emitente"),
      $"Nfe.infNFe.total.ICMSTot.vBC",
      $"Nfe.infNFe.total.ICMSTot.vNF",
    ).show(2, truncate = false)

    //  df.show(2, truncate = false)
    df.printSchema()
  }
}

//SchemaNFCe.main(Array())
