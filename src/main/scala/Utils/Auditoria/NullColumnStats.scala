package Utils.Auditoria
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class NullColumnStats(
                            column: String,
                            nonNullCount: Long,
                            nullPercentage: Double
                          )

class ColumnNullAnalyzer(df: DataFrame) {

  private val totalRows: Long = df.count()

  private lazy val stats: Seq[NullColumnStats] = {

    val aggregations =
      df.columns.map(c =>
        sum(when(col(c).isNotNull, 1).otherwise(0)).alias(c)
      )

    val resultRow =
      df.agg(aggregations.head, aggregations.tail: _*).collect()(0)

    df.columns.map { c =>
      val nonNull = resultRow.getAs[Long](c)
      val nullPct =
        if (totalRows == 0) 0.0
        else (1 - nonNull.toDouble / totalRows) * 100

      NullColumnStats(c, nonNull, nullPct)
    }
  }

  def totalColumns: Int = df.columns.length

  def totallyNullColumns: Seq[String] =
    stats.filter(_.nonNullCount == 0).map(_.column).sorted

  def highNullColumns(threshold: Double = 99.0): Seq[NullColumnStats] =
    stats
      .filter(_.nullPercentage > threshold)
      .sortBy(-_.nullPercentage)

  def printReport(threshold: Double = 99.0): Unit = {

    println("=" * 80)
    println("VERIFICANDO COLUNAS TOTALMENTE NULAS")
    println("=" * 80)

    println(s"\nTotal de colunas: $totalColumns")
    println(s"Colunas 100% nulas: ${totallyNullColumns.length}")

    totallyNullColumns.foreach(println)

    println("\n" + "=" * 80)
    println(s"COLUNAS COM ALTA TAXA DE NULIDADE (> $threshold%)")
    println("=" * 80)

    val highNull = highNullColumns(threshold)

    if (highNull.nonEmpty) {

      println(f"\n${"Coluna"}%-35s ${"% Nulos"}%12s ${"Não Nulos"}%15s")
      println("-" * 70)

      highNull.foreach { s =>
        println(f"${s.column}%-35s ${s.nullPercentage}%11.2f%% ${s.nonNullCount}%,15d")
      }

    } else {
      println("\nNão existem colunas com alta nulidade.")
    }
  }

}

//val path = "/datalake/prata/sources/dbms/dec/nfe/detIBS"
//val df = spark.read.parquet(path)
//
//val analyzer = new ColumnNullAnalyzer(df)
//
//analyzer.printReport()              // padrão 99%
//analyzer.printReport(95.0)          // custom threshold
