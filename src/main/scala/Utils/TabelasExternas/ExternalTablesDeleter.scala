package Utils.TabelasExternas

import org.apache.spark.sql.SparkSession

object ExternalTablesDeleter {

  /**
   * Método para excluir tabelas de um banco de dados específico.
   *
   * @param spark    Sessão do Spark.
   * @param database Nome do banco de dados onde as tabelas estão localizadas.
   * @param tables   Lista de nomes das tabelas a serem excluídas.
   */
  def excluirTabelas(spark: SparkSession, database: String, tables: List[String]): Unit = {
    tables.foreach { table =>
      val dropTableSql = s"DROP TABLE IF EXISTS $database.$table"
      spark.sql(dropTableSql) // Executa o comando SQL para remover a tabela
      println(s"Tabela $database.$table excluída com sucesso.")
    }
  }

  /**
   * Método principal para executar a exclusão de tabelas.
   *
   * @param args Argumentos de linha de comando (não utilizado neste exemplo).
   */
  def main(args: Array[String]): Unit = {
    // Inicializar SparkSession
    val spark = SparkSession.builder()
      .appName("Excluir tabelas Hive")
      .enableHiveSupport()
      .getOrCreate()

    // Lista de tabelas a serem removidas
    val tables = List(
      "dec_exadata_nfce_det", "dec_exadata_nfe_det", "dec_exadata_nfce_infnfce", "dec_exadata_nfe_infnfe",
      "dec_exadata_bpe_bpe", "dec_exadata_bpe_cancelamento", "dec_exadata_cte_cte", "dec_exadata_cte_cteos",
      "dec_exadata_cte_ctesimp", "dec_exadata_cte_gvte", "dec_exadata_cte_cancelamento", "dec_exadata_mdfe_mdfe",
      "dec_exadata_mdfe_cancelamento", "dec_exadata_nf3e_nf3e", "dec_exadata_nf3e_cancelamento", "dec_exadata_nfcom_cancelamento",
      "dec_exadata_nfce_cancelamento", "dec_exadata_nfe_cancelamento", "dec_exadata_nfe_evento"
    )

    // Nome do banco de dados
    val database = "seec_prdc_documento_fiscal"

    // Executar a exclusão das tabelas
    excluirTabelas(spark, database, tables)

    // Fechar a SparkSession
  //  spark.close()
  }
}

//ExternalTablesDeleter.main(Array())