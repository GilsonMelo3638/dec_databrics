package Utils.InferenciaSchema

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs.{FileSystem, Path}

object MonitoramentoDocumentoPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Pipeline Documento - Extracao e Inferencia")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .enableHiveSupport()
      .getOrCreate()

    try {

      // 🔹 Variáveis principais
      val documento = "nf3e"   // <<< ALTERA AQUI SE PRECISAR
      val ano = "2026"
      val mes = "02"

      val origemPath =
        s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

      val xmlOnlyPath =
        s"/tmp/${documento}_xml_only/year=$ano/month=$mes"

      println(s"Lendo dados de: $origemPath")

      val df = spark.read.parquet(origemPath)

      if (!df.columns.contains("XML_DOCUMENTO_CLOB")) {
        throw new Exception("Coluna XML_DOCUMENTO_CLOB não encontrada!")
      }

      val dfXml = df
        .select(col("XML_DOCUMENTO_CLOB"))
        .filter(col("XML_DOCUMENTO_CLOB").isNotNull)

      val total = dfXml.count()
      println(s"Total de XMLs ($documento): $total")

      if (total == 0) {
        println("Nenhum XML encontrado. Encerrando.")
        return
      }

      // 🔹 Salva XMLs em parquet
      dfXml.write
        .mode("overwrite")
        .parquet(xmlOnlyPath)

      println(s"XMLs salvos em: $xmlOnlyPath")

      // ===============================
      // 🔥 Inferência de Schema
      // ===============================

      val tempDir =
        s"/tmp/${documento}_xml_temp_${System.currentTimeMillis()}"

      dfXml.write
        .mode("overwrite")
        .text(tempDir)

      val xmlDF = spark.read
        .format("xml")
        .option("rowTag", s"${documento}Proc") // se o rowTag seguir padrão
        .option("inferSchema", "true")
        .load(tempDir)

      println(s"Schema inferido para $documento:")
      xmlDF.printSchema()

      // Limpeza
      FileSystem
        .get(spark.sparkContext.hadoopConfiguration)
        .delete(new Path(tempDir), true)

    } finally {
      spark.stop()
    }
  }
}

MonitoramentoDocumentoPipeline.main(Array())
