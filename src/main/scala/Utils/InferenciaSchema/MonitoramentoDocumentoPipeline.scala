package Utils.InferenciaSchema

import org.apache.spark.sql.{SparkSession, DataFrame}
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

      val documento = "nf3e"
      val tag = "nf3eProc"
      val ano = "2026"

      val mesInicio = 3
      val mesFim = 3

      var dfAcumulado: DataFrame = null

      // ===============================
      // 🔁 Loop de meses
      // ===============================

      for (mesInt <- mesInicio to mesFim) {

        val mes = f"$mesInt%02d"

        println("\n" + "=" * 80)
        println(s"PROCESSANDO MÊS: $ano-$mes")
        println("=" * 80)

        val origemPath =
          s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

        val df = spark.read.parquet(origemPath)

        if (df.columns.contains("XML_DOCUMENTO_CLOB")) {

          val dfXml = df
            .select(col("XML_DOCUMENTO_CLOB"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)

          val total = dfXml.count()
          println(s"Total de XMLs mês $mes: $total")

          if (total > 0) {
            dfAcumulado =
              if (dfAcumulado == null) dfXml
              else dfAcumulado.unionByName(dfXml)
          }

        } else {
          println("Coluna XML_DOCUMENTO_CLOB não encontrada. Pulando mês.")
        }
      }

      // ===============================
      // 🔥 Inferência única
      // ===============================

      if (dfAcumulado == null) {
        println("Nenhum XML encontrado no período.")
        return
      }

      val totalFinal = dfAcumulado.count()
      println(s"\nTotal consolidado de XMLs: $totalFinal")

      val tempDir =
        s"/tmp/${documento}_xml_temp_${System.currentTimeMillis()}"

      dfAcumulado.write
        .mode("overwrite")
        .text(tempDir)

      val xmlDF = spark.read
        .format("xml")
        .option("rowTag", tag)
        .option("inferSchema", "true")
        .load(tempDir)

      println("\nSchema inferido considerando TODOS os meses:")
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

// MonitoramentoDocumentoPipeline.main(Array())