
package Utils.InferenciaSchema.Mapeamento.IndividualIncremental

import Schemas.MDFeSchema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MDFeMap {

  def flattenSchema(schema: StructType, prefix: String = ""): Seq[String] = {
    schema.fields.flatMap { field =>

      val fieldName =
        if (prefix.isEmpty) field.name
        else s"$prefix.${field.name}"

      field.dataType match {
        case struct: StructType =>
          flattenSchema(struct, fieldName)

        case ArrayType(struct: StructType, _) =>
          flattenSchema(struct, fieldName)

        case _ =>
          Seq(fieldName)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Monitoramento Documento Pipeline - Consolidado")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .enableHiveSupport()
      .getOrCreate()

    try {

      val documento = "mdfe"
      val tag = s"${documento}Proc"
      val ano = "2026"
      val mesInicio = 3
      val mesFim = 4

      var dfAcumulado: DataFrame = null

      // ============================================
      // 🔁 LOOP → SOMENTE LEITURA + UNION
      // ============================================

      for (mesInt <- mesInicio to mesFim) {

        val mes = f"$mesInt%02d"

        println("\n" + "=" * 80)
        println(s"LENDO MÊS: $ano-$mes")
        println("=" * 80)

        val origemPath =
          s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

        val df = spark.read.parquet(origemPath)

        if (df.columns.contains("XML_DOCUMENTO_CLOB")) {

          val dfXml = df
            .select(col("XML_DOCUMENTO_CLOB"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)

          val total = dfXml.count()
          println(s"Total XMLs mês $mes: $total")

          if (total > 0) {
            dfAcumulado =
              if (dfAcumulado == null) dfXml
              else dfAcumulado.unionByName(dfXml)
          }

        } else {
          println("Coluna XML_DOCUMENTO_CLOB não encontrada.")
        }
      }

      // ============================================
      // 🔥 VALIDAÇÃO
      // ============================================

      if (dfAcumulado == null) {
        println("Nenhum XML encontrado no período.")
        return
      }

      val totalFinal = dfAcumulado.count()
      println(s"\nTOTAL CONSOLIDADO: $totalFinal")

      // ============================================
      // 🧱 ESCRITA TEMP
      // ============================================

      val tempDir =
        s"/tmp/${documento}_xml_temp_${System.currentTimeMillis()}"

      dfAcumulado.write.mode("overwrite").text(tempDir)

      // ============================================
      // 🔍 INFERÊNCIA ÚNICA
      // ============================================

      val xmlDF = spark.read
        .format("xml")
        .option("rowTag", tag)
        .option("inferSchema", "true")
        .load(tempDir)

      println("\nSchema inferido (CONSOLIDADO):")
      xmlDF.printSchema()

      // ============================================
      // 🔎 COMPARAÇÃO
      // ============================================

      val schemaInferido = flattenSchema(xmlDF.schema).toSet

      val schemaOficial = flattenSchema(
        MDFeSchema.createSchema()
      ).toSet

      val camposNaoMapeados = schemaInferido
        .diff(schemaOficial)
        .filterNot(_.toLowerCase.contains("signature"))

      println("\n" + "=" * 80)
      println("CAMPOS NÃO MAPEADOS (CONSOLIDADO)")
      println("=" * 80)

      if (camposNaoMapeados.isEmpty) {
        println("✔ Nenhuma diferença encontrada.")
      } else {
        camposNaoMapeados.toSeq.sorted.foreach(println)
      }

      // ============================================
      // 🧹 LIMPEZA
      // ============================================

      FileSystem
        .get(spark.sparkContext.hadoopConfiguration)
        .delete(new Path(tempDir), true)

    } finally {
      spark.stop()
    }
  }
}

//MDFeMap.main(Array())
