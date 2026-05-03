package Utils.CorrecaoSchemaTransferirLegado.FechamentoMensal

import org.apache.spark.sql.SparkSession
import java.time.LocalDate

object D_ContadorDocumentosLegado {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Contador de Documentos DEC")
      .getOrCreate()

    val tipos = Seq(
      "bpe_cancelamento_diario",
      "bpe_diario",
      "bpe_evento_diario",
      "cte_cancelamento_diario",
      "cte_diario",
      "cte_evento_diario",
      "mdfe_cancelamento_diario",
      "mdfe_diario",
      "mdfe_evento_diario",
      "nf3e_cancelamento_diario",
      "nf3e_diario",
      "nf3e_evento_diario",
      "nfce_cancelamento_diario",
      "nfce_diario",
      "nfcom_cancelamento_diario",
      "nfcom_diario",
      "nfcom_evento_diario",
      "nfe_cancelamento_diario",
      "nfe_diario",
      "nfe_evento_diario"
    )

    val basePath = "/datalake/bronze/sources/dbms/legado/dec/"

    // 🔥 mês anterior automático
    val dataReferencia = LocalDate.now().minusMonths(1)
    val ano = dataReferencia.getYear
    val mes = dataReferencia.getMonthValue

    var totalGeral = 0L
    val resultados = scala.collection.mutable.Map[String, Long]()

    tipos.foreach { tipo =>

      val path = s"$basePath/$tipo"

      println(s"\nProcessando tipo: $tipo")

      try {
        val df = spark.read.parquet(path)

        // 🔥 Filtra pela partição (funciona independente de 4 ou 04)
        val filtrado = df.filter(s"year = $ano AND month = $mes")

        val count = filtrado.count()

        resultados(tipo) = count
        totalGeral += count

        println(s"✔ $tipo -> $count")

      } catch {
        case e: Exception =>
          println(s"Erro em $tipo: ${e.getMessage}")
          resultados(tipo) = 0L
      }
    }

    println("\n================ RESUMO FINAL ================\n")

    resultados.toSeq.sortBy(_._1).foreach { case (tipo, count) =>
      println(f"$tipo%-30s -> $count%10d")
    }

    println("\n---------------------------------------------")
    println(f"TOTAL GERAL -> $totalGeral%,d")
    println("=============================================\n")

    spark.stop()
  }
}
//ContadorDocumentosLegado.main(Array())