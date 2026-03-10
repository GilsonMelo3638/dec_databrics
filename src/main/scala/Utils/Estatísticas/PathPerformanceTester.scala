package Utils.Estatísticas

import org.apache.spark.sql.SparkSession

class PathPerformanceTester(spark: SparkSession) {

  def testarPath(path: String, colunaDistinct: String): Unit = {

    println("\n==============================")
    println(s"Teste do path: $path")
    println(s"Coluna distinct: $colunaDistinct")
    println("==============================")

    spark.catalog.clearCache()

    val t0 = System.currentTimeMillis()

    val df = spark.read.parquet(path)

    val t1 = System.currentTimeMillis()
    df.printSchema()

    val total = df.count()

    val t2 = System.currentTimeMillis()

    val distinct = df.select(colunaDistinct).distinct().count()

    val t3 = System.currentTimeMillis()

    val tempoLeitura = (t1 - t0) / 1000.0
    val tempoCount = (t2 - t1) / 1000.0
    val tempoDistinct = (t3 - t2) / 1000.0
    val tempoTotal = (t3 - t0) / 1000.0

    println("\n------ RELATÓRIO ------")
    println(f"Tempo leitura:    $tempoLeitura%.2f s")
    println(f"Tempo count():    $tempoCount%.2f s")
    println(f"Tempo distinct(): $tempoDistinct%.2f s")
    println(f"Tempo total:      $tempoTotal%.2f s")
    println(s"Total registros:  $total")
    println(s"Distinct:         $distinct")
  }

}

//val spark = SparkSession.builder().getOrCreate()
//
//val tester = new PathPerformanceTester(spark)
//
//tester.testarPath(
//  "/datalake/bronze/sources/dbms/legado/dec/nfce_diario",
//  "nsu"
//)
//
//tester.testarPath(
//  "/datalake/bronze/sources/dbms/legado/dec/nfce_diario2",
//  "nsu"
//)