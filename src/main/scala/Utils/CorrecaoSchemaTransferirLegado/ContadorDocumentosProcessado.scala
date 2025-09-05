package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.SparkSession

object ContadorDocumentosProcessado {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Contador de Documentos DEC")
      .getOrCreate()

    val tipos = Seq("nfe", "nfce", "bpe", "cte", "mdfe", "nf3e", "nfcom")
    val basePath = "/datalake/bronze/sources/dbms/dec/diario"
    val ano = "2025"
    val mes = "08"

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
//ContadorDocumentosProcessado.main(Array())