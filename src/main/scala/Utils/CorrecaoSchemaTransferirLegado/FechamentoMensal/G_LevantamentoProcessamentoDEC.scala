package Utils.CorrecaoSchemaTransferirLegado.FechamentoMensal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object G_LevantamentoProcessamentoDEC {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Levantamento Processamento DEC")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val paths = Seq(
      "nfce/processado",
      "nfe/processado",
      "cte/processado",
      "mdfe/processado",
      "bpe/processado",
      "nf3e/processado",
      "nfcom/processado",
      "nfce_cancelamento/processado",
      "nfe_cancelamento/processado",
      "cte_cancelamento/processado",
      "mdfe_cancelamento/processado",
      "bpe_cancelamento/processado",
      "nf3e_cancelamento/processado",
      "nfcom_cancelamento/processado",
      "bpe_evento/processado",
      "nfe_evento/processado",
      "mdfe_evento/processado",
      "nf3e_evento/processado",
      "nfcom_evento/processado"
    )

    val base = "/datalake/bronze/sources/dbms/dec/processamento"
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    paths.foreach { p =>

      val fullPath = new Path(s"$base/$p")

      println("\n====================================================")
      println(s"Diretório: $p")
      println("====================================================")

      if (!fs.exists(fullPath)) {
        println("❌ Diretório não existe")
      } else {

        val status = fs.listStatus(fullPath)

        val datas = status
          .filter(_.isDirectory)
          .map(_.getPath.getName)
          .filter(_.matches("\\d{8}"))
          .flatMap { d =>
            try {
              Some(LocalDate.parse(d, formatter))
            } catch {
              case _: Exception => None
            }
          }
          .sortBy(_.toEpochDay) // 🔥 solução definitiva

        if (datas.isEmpty) {
          println("⚠ Nenhuma data encontrada")
        } else {

          val min = datas.head
          val max = datas.last
          val total = datas.size

          println(s"📅 Período: $min até $max")
          println(s"📊 Total de dias encontrados: $total")

          // 🔥 Geração de todos os dias do intervalo
          val todosDias = Iterator
            .iterate(min)(_.plusDays(1))
            .takeWhile(!_.isAfter(max))
            .toSet

          val existentes = datas.toSet

          val faltantes = (todosDias -- existentes)
            .toSeq
            .sortBy(_.toEpochDay)

          if (faltantes.nonEmpty) {
            println(s"⚠ Dias faltantes (${faltantes.size}):")

            faltantes.take(10).foreach(d => println(s" - $d"))

            if (faltantes.size > 10) {
              println(s"... e mais ${faltantes.size - 10} dias")
            }
          } else {
            println("✔ Nenhum dia faltante")
          }
        }
      }
    }

    spark.stop()
  }
}

//G_LevantamentoProcessamentoDEC.main(Array())