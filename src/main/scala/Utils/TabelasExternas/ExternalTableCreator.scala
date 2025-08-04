package Utils.TabelasExternas

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
    .set("spark.sql.hive.metastorePartitionPruning", "true")
    .set("spark.sql.hive.convertMetastoreParquet", "true")
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic") // Adicionado para melhor tratamento de partições

  // Cria o SparkSession com suporte ao Hive
  private val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  def createExternalTables(documento: String, parquetPath: String, tables: List[String]): Unit = {
    tables.foreach { table =>
      val fullTableName = s"seec_prdc_documento_fiscal.dec_exadata_${documento}_$table"

      // Verifica se a tabela já existe
      val tableExists = spark.catalog.tableExists(fullTableName)

      if (!tableExists) {
        // Se a tabela não existe, cria com schema inferido dos dados
        val df = spark.read.parquet(s"$parquetPath/$table")
        val schemaWithoutPartition = df.schema.filter(_.name != "chave_particao")
        val schemaSql = schemaWithoutPartition.map(field => s"`${field.name}` ${field.dataType.simpleString.toUpperCase}").mkString(",\n")
        val partitionColumn = "chave_particao STRING"

        val createTableSql = s"""
          CREATE EXTERNAL TABLE IF NOT EXISTS $fullTableName (
              $schemaSql
          )
          PARTITIONED BY ($partitionColumn)
          STORED AS PARQUET
          LOCATION '${parquetPath}/$table'
        """
        spark.sql(createTableSql)
      }

      // SEMPRE atualiza as partições, independente de ser criação nova ou não
      updateTablePartitions(fullTableName)
    }
  }

  def updateTablePartitions(fullTableName: String): Unit = {
    try {
      println(s"Atualizando partições para a tabela: $fullTableName")

      // 1. Primeiro fazemos o repair table para garantir que todas as partições sejam reconhecidas
      spark.sql(s"MSCK REPAIR TABLE $fullTableName")

      // 2. Força o refresh completo da tabela
      spark.sql(s"REFRESH TABLE $fullTableName")

      // 3. Invalida o cache para garantir que consultas subsequentes usem os dados mais recentes
      spark.catalog.refreshTable(fullTableName)

      // 4. Opcional: Verifica o número de partições (para logging)
      val partitions = spark.sql(s"SHOW PARTITIONS $fullTableName").count()
      println(s"Tabela $fullTableName atualizada com sucesso. Número de partições: $partitions")
    } catch {
      case e: Exception =>
        println(s"Erro ao atualizar partições da tabela $fullTableName: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Nova função para atualização em lote de todas as tabelas
  def updateAllTables(datasets: List[(String, String, List[String])]): Unit = {
    datasets.foreach { case (documento, parquetPath, tables) =>
      tables.foreach { table =>
        val fullTableName = s"seec_prdc_documento_fiscal.dec_exadata_${documento}_$table"
        updateTablePartitions(fullTableName)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val datasets = List(
      ("nfe", "hdfs:///datalake/prata/sources/dbms/dec/nfe", List("infNFe", "det", "cancelamento", "evento")),
      ("nfce", "hdfs:///datalake/prata/sources/dbms/dec/nfce", List("infNFCe", "det", "cancelamento")),
      ("bpe", "hdfs:///datalake/prata/sources/dbms/dec/bpe", List("BPe", "cancelamento")),
      ("mdfe", "hdfs:///datalake/prata/sources/dbms/dec/mdfe", List("MDFe", "cancelamento")),
      ("nf3e", "hdfs:///datalake/prata/sources/dbms/dec/nf3e", List("NF3e", "cancelamento")),
      ("cte", "hdfs:///datalake/prata/sources/dbms/dec/cte", List("CTe", "GVTe", "CTeOS", "CTeSimp", "cancelamento")),
      ("nfcom", "hdfs:///datalake/prata/sources/dbms/dec/nfcom", List("NFCom"))
    )

    // Cria ou atualiza todas as tabelas
    datasets.foreach { case (documento, parquetPath, tables) =>
      createExternalTables(documento, parquetPath, tables)
    }

    // Se quiser chamar apenas a atualização, usar:
    // updateAllTables(datasets)
  }
}

//ExternalTableCreator.main(Array())
