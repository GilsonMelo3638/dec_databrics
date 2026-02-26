package Utils.CorrecaoSchemaTransferirLegado
import org.apache.spark.sql.SparkSession


class ContadorDiario(spark: SparkSession, baseRoot: String) {

  def auditarMes(year: Int, month: Int, diasNoMes: Int = 31): Unit = {

    val basePath = s"$baseRoot/year=$year/month=$month/"

    println(s"\nAuditando: year=$year month=$month")
    println("--------------------------------------------------")

    for (d <- 1 to diasNoMes) {

      val path = s"${basePath}day=$d"

      try {

        val df = spark.read.parquet(path).select("CHAVE")

        val total = df.count()
        val distinto = df.distinct().count()
        val duplicadas = total - distinto

        println(f"Dia $d%02d -> Total: $total | Distinto: $distinto | Duplicadas: $duplicadas")

      } catch {
        case _: Exception =>
          println(f"Dia $d%02d -> Partição inexistente ou erro")
      }
    }

    println("--------------------------------------------------\n")
  }
}

//val rootPath = "/datalake/bronze/sources/dbms/legado/dec/nfe_diario2"
//
//val auditor = new ContadorDiario(spark, rootPath)
//
//auditor.auditarMes(2024, 8)