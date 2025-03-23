package Utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExternalTableCreator {
  // Configuração de autenticação LDAP para Hive
  private val ldapUsername = "svc_bigdata"
  private val ldapPassword = "@svc.bigdata1"

  // Cria uma configuração de Spark
  private val conf = new SparkConf()
    .set("spark.hadoop.hive.server2.authentication", "LDAP")
    .set("spark.hadoop.hive.server2.authentication.ldap.url", "ldap://fazenda.net:389")
    .set("spark.hadoop.hive.server2.authentication.ldap.baseDN", "OU=USUARIOS,DC=fazenda,DC=net")
    .set("spark.hadoop.hive.server2.custom.authentication.username", ldapUsername)
    .set("spark.hadoop.hive.server2.custom.authentication.password", ldapPassword)

  // Cria o SparkSession com suporte ao Hive
  private val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  def createExternalTables(documento: String, parquetPath: String, tables: List[String]): Unit = {
    tables.foreach { table =>
      val df = spark.read.parquet(s"$parquetPath/$table")
      val schemaWithoutPartition = df.schema.filter(_.name != "chave_particao")
      val schemaSql = schemaWithoutPartition.map(field => s"`${field.name}` ${field.dataType.simpleString.toUpperCase}").mkString(",\n")
      val partitionColumn = "chave_particao STRING"

      val createTableSql = s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS seec_prdc_documento_fiscal.dec_exadata_${documento}_$table (
          $schemaSql
      )
      PARTITIONED BY ($partitionColumn)
      STORED AS PARQUET
      LOCATION '${parquetPath}/$table'
      """

      spark.sql(createTableSql)
      spark.sql(s"MSCK REPAIR TABLE seec_prdc_documento_fiscal.dec_exadata_${documento}_$table")
    }
  }

  def main(args: Array[String]): Unit = {
    val datasets = List(
      ("nfe", "hdfs:///datalake/prata/sources/dbms/dec/nfe", List("infNFe", "det", "cancelamento")),
      ("nfce", "hdfs:///datalake/prata/sources/dbms/dec/nfce", List("infNFCe", "det", "cancelamento")),
      ("bpe", "hdfs:///datalake/prata/sources/dbms/dec/bpe", List("BPe", "cancelamento")),
      ("mdfe", "hdfs:///datalake/prata/sources/dbms/dec/mdfe", List("MDFe", "cancelamento")),
      ("nf3e", "hdfs:///datalake/prata/sources/dbms/dec/nf3e", List("NF3e", "cancelamento")),
      ("cte", "hdfs:///datalake/prata/sources/dbms/dec/cte", List("CTe", "GVTe", "CTeOS", "CTeSimp"))
    )

    datasets.foreach { case (documento, parquetPath, tables) =>
      createExternalTables(documento, parquetPath, tables)
    }
  }
}

//ExternalTableCreator.main(Array())