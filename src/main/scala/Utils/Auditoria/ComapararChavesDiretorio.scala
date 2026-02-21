package Utils.Auditoria

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.{Try, Success, Failure}

// =====================================
// Case class de resultado
// =====================================

case class ResultadoProcessamento(
                                   totalOrigem: Long,
                                   totalComparacao: Long,
                                   totalFaltantes: Long,
                                   duracaoSegundos: Long,
                                   sucesso: Boolean,
                                   mensagem: String
                                 )

// =====================================
// Classe Reutilizável
// =====================================

class ComapararChavesDiretorio(spark: SparkSession) {

  spark.sparkContext.setLogLevel("WARN")
  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private def pathExiste(path: String): Boolean =
    fs.exists(new Path(path))

  def executar(
                pathOrigem: String,
                pathComparar: String,
                colunaChave: String,
                pathOutput: String,
                modoWrite: String = "append",
                broadcastComparacao: Boolean = false
              ): ResultadoProcessamento = {

    val inicio = System.currentTimeMillis()

    Try {

      require(pathExiste(pathOrigem), s"Path origem não existe: $pathOrigem")
      require(pathExiste(pathComparar), s"Path comparação não existe: $pathComparar")

      val dfOrigem = spark.read.parquet(pathOrigem)

      val dfComparacaoBase = spark.read.parquet(pathComparar)
        .select(colunaChave)
        .distinct()

      val dfComparacao =
        if (broadcastComparacao) broadcast(dfComparacaoBase)
        else dfComparacaoBase

      val dfFaltantes =
        dfOrigem.join(dfComparacao, Seq(colunaChave), "left_anti")

      val totalOrigem = dfOrigem.count()
      val totalComparacao = dfComparacaoBase.count()
      val totalFaltantes = dfFaltantes.count()

      dfFaltantes.write
        .mode(modoWrite)
        .option("compression", "lz4")
        .parquet(pathOutput)

      val fim = System.currentTimeMillis()

      ResultadoProcessamento(
        totalOrigem,
        totalComparacao,
        totalFaltantes,
        (fim - inicio) / 1000,
        sucesso = true,
        mensagem = "Processamento concluído com sucesso"
      )

    } match {

      case Success(resultado) =>
        println(s"[SUCESSO] $resultado")
        resultado

      case Failure(ex) =>
        val fim = System.currentTimeMillis()
        val erro = ResultadoProcessamento(
          0, 0, 0,
          (fim - inicio) / 1000,
          sucesso = false,
          mensagem = ex.getMessage
        )
        println(s"[ERRO] ${ex.getMessage}")
        erro
    }
  }
}

// =====================================
// Object executável
// =====================================

object ComapararChavesDiretorio {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Documento Faltante Produção")
      .enableHiveSupport()
      .getOrCreate()

    // ================================
    // Argumentos (ou padrão)
    // ================================

    val pathOrigem   = if (args.length > 0) args(0)
    else "/datalake/bronze/sources/dbms/dec/cte/202502"

    val pathComparar = if (args.length > 1) args(1)
    else "/datalake/bronze/sources/dbms/legado/dec/cte_diario/year=2025/month=02"

    val colunaChave  = if (args.length > 2) args(2)
    else "CHAVE"

    val pathOutput   = if (args.length > 3) args(3)
    else "/datalake/bronze/sources/dbms/dec/cte/faltante2"

    val processor = new ComapararChavesDiretorio(spark)

    processor.executar(
      pathOrigem = pathOrigem,
      pathComparar = pathComparar,
      colunaChave = colunaChave,
      pathOutput = pathOutput,
      modoWrite = "append",
      broadcastComparacao = true
    )

  }
}

//ComapararChavesDiretorio.main(Array())