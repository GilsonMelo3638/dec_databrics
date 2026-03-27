package Utils.Auditoria

object AuditoriaDetVsDetIBS {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Auditoria Det vs DetIBS")
      .getOrCreate()

    // Paths (pode externalizar depois via args)
    val baseDet    = "/datalake/prata/sources/dbms/dec/nfce/det"
    val baseDetIBS = "/datalake/prata/sources/dbms/dec/nfce/detIBS"

    println("🔎 Iniciando auditoria de CHAVE distinta por partição...")

    // Leitura
    val dfDet    = spark.read.parquet(baseDet)
    val dfDetIBS = spark.read.parquet(baseDetIBS)

    // Agregação DET
    val detAgg = dfDet
      .select("chave_particao", "CHAVE")
      .distinct()
      .groupBy("chave_particao")
      .count()
      .withColumnRenamed("count", "qtd_det")

    // Agregação DETIBS
    val detIBSAgg = dfDetIBS
      .select("chave_particao", "CHAVE")
      .distinct()
      .groupBy("chave_particao")
      .count()
      .withColumnRenamed("count", "qtd_detIBS")

    // Join e comparação
    val resultado = detAgg
      .join(detIBSAgg, Seq("chave_particao"), "full")
      .na.fill(0)
      .withColumn("diferenca", col("qtd_det") - col("qtd_detIBS"))
      .orderBy("chave_particao")

    println("📊 Resultado completo:")
    resultado.show(50, false)

    println("⚠️ Apenas divergências:")
    resultado
      .filter(col("diferenca") =!= 0)
      .orderBy(desc("diferenca"))
      .show(false)

    println("✅ Auditoria finalizada.")

    spark.stop()
  }
}