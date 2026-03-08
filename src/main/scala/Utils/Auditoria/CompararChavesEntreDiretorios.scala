package Utils.Auditoria
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.{Failure, Success, Try}

case class ResultadoProcessamento(
                                   totalOrigem: Long,
                                   totalComparacao: Long,
                                   totalFaltantes: Long,
                                   duracaoSegundos: Long,
                                   sucesso: Boolean,
                                   mensagem: String
                                 )

class CompararChavesEntreDiretorios(spark: SparkSession) {

  spark.sparkContext.setLogLevel("WARN")

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private def pathExiste(path: String): Boolean =
    fs.exists(new Path(path))

  def executar(
                pathOrigem: String,
                pathsComparar: Seq[String],
                colunaChave: String,
                pathOutput: String
              ): ResultadoProcessamento = {

    val inicio = System.currentTimeMillis()

    Try {

      require(pathExiste(pathOrigem), s"Path origem não existe: $pathOrigem")

      // validar diretórios de comparação
      pathsComparar.foreach(p =>
        require(pathExiste(p), s"Path comparação não existe: $p")
      )

      // origem
      val origem = spark.read.parquet(pathOrigem)
        .select(col(colunaChave))
        .repartition(col(colunaChave))
        .distinct()
        .persist()

      // comparação (múltiplos meses)
      val comparar = spark.read
        .option("basePath", "/datalake/bronze/sources/dbms/legado/dec/cte_diario")
        .parquet(pathsComparar:_*)
        .select(col(colunaChave))
        .repartition(col(colunaChave))
        .distinct()
        .persist()

      val totalOrigem = origem.count()
      val totalComparacao = comparar.count()

      val faltantes = origem.except(comparar)

      val totalFaltantes = faltantes.count()

      faltantes.write
        .mode("append")
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

object CompararChavesEntreDiretorios {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Comparador de Chaves NFCe")
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 400)

    val pathOrigem =
      if (args.length > 0) args(0)
      else "/datalake/bronze/sources/dbms/legado/dec/cte_diario_complemento"

    val pathsComparar =
      if (args.length > 1)
        args(1).split(",").toSeq
      else Seq(
        "/datalake/bronze/sources/dbms/legado/dec/cte_diario/year=2025/month=2",
        "/datalake/bronze/sources/dbms/legado/dec/cte_diario/year=2025/month=3"
      )

    val colunaChave =
      if (args.length > 2) args(2)
      else "CHAVE"

    val pathOutput =
      if (args.length > 3) args(3)
      else "/datalake/bronze/sources/dbms/legado/dec/cte_diario_complemento_x"

    val processor = new CompararChavesEntreDiretorios(spark)

    processor.executar(
      pathOrigem,
      pathsComparar,
      colunaChave,
      pathOutput
    )
  }
}

// CompararChavesEntreDiretorios.main(Array())