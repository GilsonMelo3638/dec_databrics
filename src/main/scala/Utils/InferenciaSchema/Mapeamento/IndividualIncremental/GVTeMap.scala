package Utils.InferenciaSchema.Mapeamento.IndividualIncremental

import Schemas.GVTeSchema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GVTeMap {

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
      .appName("Monitoramento Documento Pipeline - CTe")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .enableHiveSupport()
      .getOrCreate()

    try {

      val documento = "cte"
      val tag = "GTVeProc"
      val ano = "2026"
      val mesInicio = 3
      val mesFim = 4

      var dfAcumulado: DataFrame = null
      var totalGeral: Long = 0

      // ============================================
      // 🔁 LOOP COM FILTRO
      // ============================================

      for (mesInt <- mesInicio to mesFim) {

        val mes = f"$mesInt%02d"

        println("\n" + "=" * 80)
        println(s"📅 PROCESSANDO MÊS: $ano-$mes")
        println("=" * 80)

        val origemPath =
          s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

        println(s"📂 Path: $origemPath")

        val df = spark.read.parquet(origemPath)

        if (df.columns.contains("XML_DOCUMENTO_CLOB") && df.columns.contains("MODELO")) {

          val dfXml = df
            .filter(col("MODELO").cast("string") === "64")               // ✔ modelo 57
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)                 // ✔ não nulo
            .filter(col("XML_DOCUMENTO_CLOB").contains("<GTVeProc"))      // ✔ tag correta
            .select(col("XML_DOCUMENTO_CLOB"))                           // ✔ só XML

          val totalMes = dfXml.count()
          println(s"📄 XMLs válidos mês $mes: $totalMes")

          if (totalMes > 0) {

            totalGeral += totalMes

            dfAcumulado =
              if (dfAcumulado == null) dfXml
              else dfAcumulado.unionByName(dfXml)

            println(s"✅ Mês $mes adicionado ao acumulado")
          } else {
            println(s"⚠️ Nenhum XML válido no mês $mes")
          }

        } else {
          println("❌ Colunas necessárias não encontradas (XML_DOCUMENTO_CLOB ou MODELO)")
        }
      }

      // ============================================
      // 🔥 VALIDAÇÃO
      // ============================================

      println("\n" + "=" * 80)
      println(s"📦 TOTAL CONSOLIDADO DE XMLs: $totalGeral")
      println("=" * 80)

      if (dfAcumulado == null) {
        println("❌ Nenhum XML encontrado no período.")
        return
      }

      // ============================================
      // 🧱 ESCRITA TEMP
      // ============================================

      val tempDir =
        s"/tmp/${documento}_xml_temp_${System.currentTimeMillis()}"

      println(s"\n🧱 Gravando XMLs temporários em: $tempDir")

      dfAcumulado.write.mode("overwrite").text(tempDir)

      // ============================================
      // 🔍 INFERÊNCIA
      // ============================================

      println("\n🔍 Iniciando inferência de schema...")

      val xmlDF = spark.read
        .format("xml")
        .option("rowTag", tag)
        .option("inferSchema", "true")
        .load(tempDir)

      println("\n📐 SCHEMA INFERIDO (CONSOLIDADO):")
      xmlDF.printSchema()

      // ============================================
      // 🔎 COMPARAÇÃO
      // ============================================

      val schemaInferido = flattenSchema(xmlDF.schema).toSet

      val schemaOficial = flattenSchema(
        GVTeSchema.createSchema()
      ).toSet

      val camposNaoMapeados = schemaInferido
        .diff(schemaOficial)
        .filterNot(_.toLowerCase.contains("signature"))

      println("\n" + "=" * 80)
      println("🔎 CAMPOS NÃO MAPEADOS (CONSOLIDADO)")
      println("=" * 80)

      if (camposNaoMapeados.isEmpty) {
        println("✔ Nenhuma diferença encontrada.")
      } else {
        camposNaoMapeados.toSeq.sorted.foreach(println)
      }

      // ============================================
      // 🧹 LIMPEZA
      // ============================================

      println(s"\n🧹 Limpando diretório temporário: $tempDir")

      FileSystem
        .get(spark.sparkContext.hadoopConfiguration)
        .delete(new Path(tempDir), true)

      println("\n✅ Processo finalizado com sucesso.")

    } finally {
      spark.stop()
    }
  }
}

// GVTeMap.main(Array())
