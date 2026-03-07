package Utils.CorrecaoDados
import org.apache.spark.sql.{SparkSession, DataFrame}

class RemoverColunaParquet(spark: SparkSession) {

  def executar(
                origem: String,
                destino: String,
                colunaRemover: String,
                colunaParticao: String = "chave_particao"
              ): Unit = {

    println(s"Iniciando processamento...")
    println(s"Origem: $origem")
    println(s"Destino: $destino")
    println(s"Coluna a remover: $colunaRemover")

    val df = spark.read.parquet(origem)

    if (!df.columns.contains(colunaRemover)) {
      println(s"Coluna $colunaRemover não existe. Nada a fazer.")
      return
    }

    val dfTratado = df.drop(colunaRemover)

    dfTratado.write
      .mode("overwrite")
      .format("parquet")
      .option("compression", "lz4")
      .option("parquet.block.size", 256 * 1024 * 1024)
      .partitionBy(colunaParticao)
      .save(destino)

    println("Regravação concluída.")

    verificarResultado(destino, colunaRemover, colunaParticao)
  }

  private def verificarResultado(
                                  destino: String,
                                  colunaRemover: String,
                                  colunaParticao: String
                                ): Unit = {

    println("Iniciando verificações...")

    val df_check = spark.read.parquet(destino)

    println(s"Colunas: ${df_check.columns.mkString(", ")}")

    if (df_check.columns.contains(colunaRemover)) {
      println(s"ERRO: coluna $colunaRemover ainda existe.")
    } else {
      println(s"OK: coluna $colunaRemover removida.")
    }

    val total = df_check.count()
    println(s"Total de registros: $total")

    val particoes = df_check.select(colunaParticao).distinct().count()
    println(s"Partições distintas: $particoes")

    println("Verificação finalizada.")
  }
}

//val job = new RemoverColunaParquet(spark)
//
//job.executar(
//  "/datalake/prata/sources/dbms/dec/nfe/infNFe",
//  "/datalake/prata/sources/dbms/dec/nfe/infNFe3",
//  "retirada_modfrete"
//)