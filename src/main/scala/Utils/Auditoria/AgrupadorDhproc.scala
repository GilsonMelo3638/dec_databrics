package Utils.Auditoria

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

class AgrupadorDhproc(spark: SparkSession) {

  spark.sparkContext.setLogLevel("WARN")

  def executar(path: String): DataFrame = {

    val df = spark.read.parquet(path)

    // PASSO 1: Criar coluna de data
    val df1 = df.withColumn(
      "data_yyyyMMdd",
      date_format(
        to_timestamp(col("DHPROC"), "dd/MM/yyyy HH:mm:ss"),
        "yyyyMMdd"
      )
    )

    println("Schema do df1 (deve ter a nova coluna):")
    df1.printSchema()

    // PASSO 2: Agrupar
    val resultado = df1
      .groupBy("data_yyyyMMdd")
      .agg(count("CHAVE").alias("quantidade"))
      .orderBy("data_yyyyMMdd")

    println("\nSchema do resultado:")
    resultado.printSchema()

    println("\nMostrando resultado:")
    resultado.show(50, false)

    println("\nContagem de linhas:")
    println(s"DataFrame original: ${df.count()} linhas")
    println(s"DataFrame agrupado: ${resultado.count()} linhas (dias com registros)")

    resultado
  }
}
object AgrupadorDhproc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Agrupador DHPROC")
      .enableHiveSupport()
      .getOrCreate()

    val path =
      if (args.length > 0) args(0)
      else "/datalake/bronze/sources/dbms/legado/dec/cte_diario_complemento"

    val agrupador = new AgrupadorDhproc(spark)

    agrupador.executar(path)

  }
}

//AgrupadorDhproc.main(Array())