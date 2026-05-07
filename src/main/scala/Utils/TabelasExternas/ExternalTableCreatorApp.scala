package Utils.TabelasExternas

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExternalTableCreatorApp {

  // =========================================================
  // CONFIGURAÇÃO LDAP
  // =========================================================

  private val ldapUsername = "svc_bigdata"
  private val ldapPassword = "@svc.bigdata1"

  // =========================================================
  // CONFIGURAÇÃO SPARK
  // =========================================================

  private val conf = new SparkConf()
    .set("spark.hadoop.hive.server2.authentication", "LDAP")
    .set("spark.hadoop.hive.server2.authentication.ldap.url", "ldap://fazenda.net:389")
    .set("spark.hadoop.hive.server2.authentication.ldap.baseDN", "OU=USUARIOS,DC=fazenda,DC=net")
    .set("spark.hadoop.hive.server2.custom.authentication.username", ldapUsername)
    .set("spark.hadoop.hive.server2.custom.authentication.password", ldapPassword)
    .set("spark.sql.hive.metastorePartitionPruning", "true")
    .set("spark.sql.hive.convertMetastoreParquet", "true")
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  // =========================================================
  // SPARK SESSION
  // =========================================================

  private val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  // =========================================================
  // CRIAÇÃO SEGURA DAS TABELAS EXTERNAS
  // =========================================================

  def createExternalTables(
                            documento: String,
                            parquetPath: String,
                            tables: List[String]
                          ): Unit = {

    tables.foreach { table =>

      val fullTableName =
        s"seec_prdc_documento_fiscal.dec_exadata_${documento}_$table"

      val tempTableName =
        s"${fullTableName}_tmp"

      try {

        println(s"====================================================")
        println(s"Iniciando processamento da tabela: $fullTableName")
        println(s"====================================================")

        // =========================================================
        // REMOVE TEMPORÁRIA ANTIGA
        // =========================================================

        spark.sql(s"DROP TABLE IF EXISTS $tempTableName")

        // =========================================================
        // LEITURA DOS PARQUETS
        // =========================================================

        val df: DataFrame =
          spark.read.parquet(s"$parquetPath/$table")

        // =========================================================
        // REMOVE COLUNA DE PARTIÇÃO DO SCHEMA
        // =========================================================

        val schemaWithoutPartition =
          df.schema.filter(_.name != "chave_particao")

        // =========================================================
        // GERA SQL DO SCHEMA
        // catalogString é MUITO mais seguro que simpleString
        // =========================================================

        val schemaSql =
          schemaWithoutPartition
            .map(field =>
              s"`${field.name}` ${field.dataType.catalogString}"
            )
            .mkString(",\n")

        // =========================================================
        // SQL CREATE TABLE TEMPORÁRIA
        // =========================================================

        val createTableSql =
          s"""
          CREATE EXTERNAL TABLE $tempTableName (
            $schemaSql
          )
          PARTITIONED BY (
            `chave_particao` STRING
          )
          STORED AS PARQUET
          LOCATION '${parquetPath}/$table'
          """

        println(s"Criando tabela temporária: $tempTableName")

        spark.sql(createTableSql)

        println(s"Tabela temporária criada com sucesso.")

        // =========================================================
        // REPAIR TABLE
        // =========================================================

        updateTablePartitions(tempTableName)

        println(s"Partições atualizadas com sucesso.")

        // =========================================================
        // REMOVE ORIGINAL SOMENTE APÓS SUCESSO
        // =========================================================

        if (spark.catalog.tableExists(fullTableName)) {

          println(s"Removendo tabela original: $fullTableName")

          spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
        }

        // =========================================================
        // RENOMEIA TEMP -> FINAL
        // =========================================================

        println(s"Renomeando tabela temporária para definitiva.")

        spark.sql(
          s"ALTER TABLE $tempTableName RENAME TO $fullTableName"
        )

        println(s"Tabela final criada com sucesso: $fullTableName")

      } catch {

        case e: Exception =>

          println(s"ERRO ao processar tabela: $fullTableName")
          println(s"Mensagem: ${e.getMessage}")

          e.printStackTrace()

          // =========================================================
          // LIMPEZA DA TEMPORÁRIA
          // =========================================================

          try {

            spark.sql(s"DROP TABLE IF EXISTS $tempTableName")

            println(s"Tabela temporária removida.")

          } catch {

            case cleanupError: Exception =>

              println(
                s"Erro ao remover temporária: ${cleanupError.getMessage}"
              )
          }
      }
    }
  }

  // =========================================================
  // ATUALIZA PARTIÇÕES
  // =========================================================

  def updateTablePartitions(fullTableName: String): Unit = {

    try {

      println(s"Atualizando partições: $fullTableName")

      // =========================================================
      // REPAIR TABLE
      // =========================================================

      spark.sql(
        s"MSCK REPAIR TABLE $fullTableName"
      )

      // =========================================================
      // REFRESH
      // =========================================================

      spark.sql(
        s"REFRESH TABLE $fullTableName"
      )

      spark.catalog.refreshTable(fullTableName)

      // =========================================================
      // CONTAGEM DE PARTIÇÕES
      // =========================================================

      val partitions =
        spark.sql(
          s"SHOW PARTITIONS $fullTableName"
        ).count()

      println(
        s"Tabela $fullTableName atualizada com sucesso."
      )

      println(
        s"Quantidade de partições: $partitions"
      )

    } catch {

      case e: Exception =>

        println(
          s"Erro ao atualizar partições da tabela $fullTableName"
        )

        println(e.getMessage)

        e.printStackTrace()
    }
  }

  // =========================================================
  // ATUALIZA TODAS AS TABELAS
  // =========================================================

  def updateAllTables(
                       datasets: List[(String, String, List[String])]
                     ): Unit = {

    datasets.foreach {
      case (documento, parquetPath, tables) =>

        tables.foreach { table =>

          val fullTableName =
            s"seec_prdc_documento_fiscal.dec_exadata_${documento}_$table"

          updateTablePartitions(fullTableName)
        }
    }
  }

  // =========================================================
  // MAIN
  // =========================================================

  def main(args: Array[String]): Unit = {

    val datasets = List(

      (
        "bpe",
        "hdfs:///datalake/prata/sources/dbms/dec/bpe",
        List(
          "BPe",
          "cancelamento",
          "evento"
        )
      ),

      (
        "mdfe",
        "hdfs:///datalake/prata/sources/dbms/dec/mdfe",
        List(
          "MDFe",
          "cancelamento",
          "evento"
        )
      ),

      (
        "nf3e",
        "hdfs:///datalake/prata/sources/dbms/dec/nf3e",
        List(
          "NF3e",
          "cancelamento",
          "evento"
        )
      ),

      (
        "cte",
        "hdfs:///datalake/prata/sources/dbms/dec/cte",
        List(
          "CTe",
          "CTeOS",
          "CTeSimp",
          //"GVTe",
          "cancelamento",
          "evento"
        )
      ),

      (
        "nfcom",
        "hdfs:///datalake/prata/sources/dbms/dec/nfcom",
        List(
          "NFCom",
          "cancelamento",
          "evento"
        )
      )
    )

    // =========================================================
    // EXECUÇÃO
    // =========================================================

    datasets.foreach {
      case (documento, parquetPath, tables) =>

        createExternalTables(
          documento,
          parquetPath,
          tables
        )
    }

    println("Processamento concluído.")

  }
}

// =========================================================
// EXECUÇÃO MANUAL
// =========================================================

// ExternalTableCreatorApp.main(Array())