package Utils.CorrecaoSchemaTransferirLegado.FechamentoMensal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object H_LevantamentoParticoesAno {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Levantamento Partições Ano 2026")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val paths = Seq(
      "nfe",
      "nfce",
      "bpe",
      "cte",
      "mdfe",
      "nf3e",
      "nfcom",
      "nfce_cancelamento",
      "nfe_cancelamento",
      "cte_cancelamento",
      "mdfe_cancelamento",
      "bpe_cancelamento",
      "nf3e_cancelamento",
      "nfcom_cancelamento",
      "bpe_evento",
      "nfe_evento",
      "mdfe_evento",
      "nf3e_evento",
      "nfcom_evento"
    )

    val base = "/datalake/bronze/sources/dbms/dec/diario"
    val ano = "2026"

    paths.foreach { tipo =>

      val fullPath = new Path(s"$base/$tipo/year=$ano")

      println("\n====================================================")
      println(s"Tipo: $tipo")
      println("====================================================")

      if (!fs.exists(fullPath)) {
        println("❌ Diretório não existe")
      } else {

        val status = fs.listStatus(fullPath)

        val meses = status
          .filter(_.isDirectory)
          .map(_.getPath.getName)
          .filter(_.startsWith("month="))
          .flatMap { m =>
            val valor = m.replace("month=", "")
            try {
              Some(valor.toInt) // 🔥 resolve 4 e 04
            } catch {
              case _: Exception => None
            }
          }
          .distinct
          .sorted

        if (meses.isEmpty) {
          println("⚠ Nenhuma partição encontrada")
        } else {
          println(s"📊 Total de meses: ${meses.size}")
          println(s"📅 Meses encontrados: ${meses.mkString(", ")}")
        }
      }
    }

    spark.stop()
  }
}

//H_LevantamentoParticoesAno.main(Array())