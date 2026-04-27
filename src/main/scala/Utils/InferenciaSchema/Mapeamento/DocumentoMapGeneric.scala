package Utils.InferenciaSchema.Mapeamento

import Schemas._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DocumentoMapGeneric {

  // ============================================
  // 🔍 FLATTEN
  // ============================================
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

  // ============================================
  // ⚙️ EXECUTOR GENÉRICO (CORRIGIDO)
  // ============================================
  def executar(
                spark: SparkSession,
                documento: String,
                tag: String,
                schemaOficial: StructType,
                ano: String,
                mesInicio: Int,
                mesFim: Int,
                aplicarFiltro: DataFrame => DataFrame,
                filtroCampos: String => Boolean
              ): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var dfAcumulado: DataFrame = null
    var totalGeral: Long = 0

    for (mesInt <- mesInicio to mesFim) {

      val mes = f"$mesInt%02d"

      val path =
        s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

      println(s"\n📅 $documento | $ano-$mes")
      println(s"📂 Path: $path")

      // 🔥 valida path
      if (!fs.globStatus(new Path(path)).nonEmpty) {
        println("⚠️ Path não encontrado")
      } else {

        val df = spark.read.parquet(path)

        if (df.columns.contains("XML_DOCUMENTO_CLOB")) {

          val dfFiltrado = aplicarFiltro(df).cache()

          val totalMes = dfFiltrado.count()
          println(s"📄 XMLs válidos: $totalMes")

          if (totalMes > 0) {

            totalGeral += totalMes

            dfAcumulado =
              if (dfAcumulado == null) dfFiltrado
              else dfAcumulado.unionByName(dfFiltrado)
          }

        } else {
          println("❌ Coluna XML_DOCUMENTO_CLOB não encontrada")
        }
      }
    }

    if (dfAcumulado == null) {
      println("❌ Nenhum XML encontrado.")
      return
    }

    println(s"\n📦 TOTAL CONSOLIDADO: $totalGeral")

    val tempDir =
      s"/tmp/${documento}_xml_${System.currentTimeMillis()}"

    dfAcumulado.write.mode("overwrite").text(tempDir)

    val xmlDF = spark.read
      .format("xml")
      .option("rowTag", tag)
      .option("inferSchema", "true")
      .load(tempDir)

    println("\n📐 SCHEMA INFERIDO:")
    xmlDF.printSchema()

    val schemaInferido = flattenSchema(xmlDF.schema).toSet
    val schemaOficialFlat = flattenSchema(schemaOficial).toSet

    val diff = schemaInferido
      .diff(schemaOficialFlat)
      .filter(filtroCampos)

    println("\n🔎 CAMPOS NÃO MAPEADOS")

    if (diff.isEmpty) println("✔ Nenhuma diferença")
    else diff.toSeq.sorted.foreach(println)

    fs.delete(new Path(tempDir), true)

    println("\n✅ Finalizado")
  }

  // ============================================
  // 🎯 MAIN
  // ============================================
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DocumentoMapGeneric")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    try {

      val ano = "2026"
      val mesInicio = 3
      val mesFim = 4

      // =========================
      // 🔧 FUNÇÕES PADRÃO
      // =========================
      val filtroPadrao = (df: DataFrame) =>
        df.filter(col("XML_DOCUMENTO_CLOB").isNotNull)
          .select(col("XML_DOCUMENTO_CLOB"))

      val ignoraSignature = (campo: String) =>
        !campo.toLowerCase.contains("signature")

      // =========================
      // CTE
      // =========================
      executar(
        spark, "cte", "cteProc",
        CTeSchema.createSchema(),
        ano, mesInicio, mesFim,
        df =>
          df.filter(col("MODELO").cast("string") === "57")
            .filter(col("XML_DOCUMENTO_CLOB").rlike("<cteProc"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
            .select(col("XML_DOCUMENTO_CLOB")),
        ignoraSignature
      )

      // =========================
      // CTE SIMP
      // =========================
      executar(
        spark, "cte", "cteSimpProc",
        CTeSimpSchema.createSchema(),
        ano, mesInicio, mesFim,
        df =>
          df.filter(col("MODELO").cast("string") === "57")
            .filter(col("XML_DOCUMENTO_CLOB").rlike("<cteSimpProc"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
            .select(col("XML_DOCUMENTO_CLOB")),
        ignoraSignature
      )

      // =========================
      // CTE OS
      // =========================
      executar(
        spark, "cte", "cteOSProc",
        CTeOSSchema.createSchema(),
        ano, mesInicio, mesFim,
        df =>
          df.filter(col("MODELO").cast("string") === "67")
            .filter(col("XML_DOCUMENTO_CLOB").rlike("<cteOSProc"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
            .select(col("XML_DOCUMENTO_CLOB")),
        ignoraSignature
      )

      // =========================
      // CTE GTVe
      // =========================
      executar(
        spark, "cte", "GTVeProc",
        GVTeSchema.createSchema(),
        ano, mesInicio, mesFim,
        df =>
          df.filter(col("MODELO").cast("string") === "64")
            .filter(col("XML_DOCUMENTO_CLOB").rlike("<GTVeProc"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
            .select(col("XML_DOCUMENTO_CLOB")),
        ignoraSignature
      )

      // =========================
      // MDFE / BPE / NF3E / NFCOM
      // =========================
      executar(spark,"mdfe","mdfeProc",MDFeSchema.createSchema(),ano,mesInicio,mesFim,filtroPadrao,ignoraSignature)
      executar(spark,"bpe","bpeProc",BPeSchema.createSchema(),ano,mesInicio,mesFim,filtroPadrao,ignoraSignature)
      executar(spark,"nf3e","nf3eProc",NF3eSchema.createSchema(),ano,mesInicio,mesFim,filtroPadrao,ignoraSignature)
      executar(spark,"nfcom","nfcomProc",NFComSchema.createSchema(),ano,mesInicio,mesFim,filtroPadrao,ignoraSignature)

      // =========================
      // NFE - INF
      // =========================
      executar(
        spark, "nfe", "nfeProc",
        NFeSchema.createSchema(),
        ano, mesInicio, mesFim,
        filtroPadrao,
        campo => {
          val l = campo.toLowerCase
          !l.contains("signature") && !l.split("\\.").contains("det")
        }
      )

      // =========================
      // NFE - DET
      // =========================
      executar(
        spark, "nfe", "nfeProc",
        NFeDetSchema.createSchema(),
        ano, mesInicio, mesFim,
        filtroPadrao,
        campo => {
          val partes = campo.toLowerCase.split("\\.")
          partes.contains("det") &&
            !partes.contains("signature") &&
            !partes.contains("infnfe")
        }
      )

      // =========================
      // NFCE - INF
      // =========================
      executar(
        spark, "nfce", "nfeProc",
        NFCeSchema.createSchema(),
        ano, mesInicio, mesFim,
        filtroPadrao,
        campo => {
          val l = campo.toLowerCase
          !l.contains("signature") && !l.split("\\.").contains("det")
        }
      )

      // =========================
      // NFCE - DET
      // =========================
      executar(
        spark, "nfce", "nfeProc",
        NFCeDetSchema.createSchema(),
        ano, mesInicio, mesFim,
        filtroPadrao,
        campo => {
          val partes = campo.toLowerCase.split("\\.")
          partes.contains("det") &&
            !partes.contains("signature") &&
            !partes.contains("infnfe")
        }
      )

    } finally {
      spark.stop()
    }
  }
}

DocumentoMapGeneric.main(Array())