package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.SparkSession

object ContadorDocumentosLegado {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Contador de Documentos DEC")
      .getOrCreate()

    val tipos = Seq("nfe_diario", "nfce_diario", "nf3e_diario", "nfcom_diario","bpe_diario", "cte_diario", "mdfe_diario",
      "nfe_cancelamento_diario", "nfce_cancelamento_diario", "bpe_cancelamento_diario", "cte_cancelamento_diario",
      "mdfe_cancelamento_diario", "nfcom_cancelamento_diario", "nf3e_cancelamento_diario",
      "nfe_evento_diario", "nfcom_evento_diario","mdfe_evento_diario", "nf3e_evento_diario"
    )
    val basePath = "/datalake/bronze/sources/dbms/legado/dec/"
    val ano = "2025"
    val mes = "11"

    var totalCount = 0L

    tipos.foreach { tipo =>
      val path = s"$basePath/$tipo/year=$ano/month=$mes"
      println(s"\nLendo arquivos do tipo: $tipo")
      try {
        val df = spark.read.parquet(path)
        df.printSchema()
        val count = df.count()
        println(s"Total de documentos $tipo: $count")
        totalCount += count
      } catch {
        case e: Exception =>
          println(s"Erro ao ler dados do tipo $tipo no caminho $path: ${e.getMessage}")
      }
    }

    println(s"\n==============================")
    println(s"Total geral de documentos: $totalCount")
    println(s"==============================\n")

    spark.stop()
  }
}
//ContadorDocumentosLegado.main(Array())