package Utils.InferenciaSchema.Mapeamento.IndividualIncremental

import Schemas.BPeTASchema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BPeTAMap {

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
      .appName("Monitoramento Documento Pipeline - BPeTA")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .enableHiveSupport()
      .getOrCreate()

    try {

      val documento = "bpe"

      // tag raiz do XML externo
      val tag = s"${documento}Proc"

      val ano = "2026"
      val mesInicio = 5
      val mesFim = 6

      var dfAcumulado: DataFrame = null

      println("=" * 100)
      println("INÍCIO PROCESSAMENTO BPeTA")
      println("=" * 100)

      // =====================================================
      // LEITURA DOS PARQUETS
      // =====================================================

      for (mesInt <- mesInicio to mesFim) {

        val mes = f"$mesInt%02d"

        println("\n" + "=" * 100)
        println(s"LENDO MÊS: $ano-$mes")
        println("=" * 100)

        val origemPath =
          s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

        println(s"Caminho: $origemPath")

        val df = spark.read.parquet(origemPath)

        if (df.columns.contains("XML_DOCUMENTO_CLOB")) {

          val dfXml = df
            .select(col("XML_DOCUMENTO_CLOB"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
            .filter(
              col("XML_DOCUMENTO_CLOB")
                .rlike("<BPeTA[\\s>]")
            )

          val total = dfXml.count()

          println(s"Total XMLs BPeTA encontrados em $mes: $total")

          if (total > 0) {

            dfAcumulado =
              if (dfAcumulado == null)
                dfXml
              else
                dfAcumulado.unionByName(dfXml)
          }

        } else {

          println(
            s"Coluna XML_DOCUMENTO_CLOB não encontrada para $ano-$mes"
          )
        }
      }

      // =====================================================
      // VALIDAÇÃO
      // =====================================================

      if (dfAcumulado == null) {

        println("Nenhum XML BPeTA encontrado no período.")

        return
      }

      val totalFinal = dfAcumulado.count()

      println("\n" + "=" * 100)
      println(s"TOTAL CONSOLIDADO BPeTA: $totalFinal")
      println("=" * 100)

      // =====================================================
      // GRAVA TEMPORÁRIO
      // =====================================================

      val tempDir =
        s"/tmp/bpeta_xml_temp_${System.currentTimeMillis()}"

      println(s"Gravando XMLs temporários em: $tempDir")

      dfAcumulado
        .write
        .mode("overwrite")
        .text(tempDir)

      // =====================================================
      // INFERÊNCIA DE SCHEMA
      // =====================================================

      println("\n" + "=" * 100)
      println("INFERINDO SCHEMA")
      println("=" * 100)

      val xmlDF = spark.read
        .format("xml")
        .option("rowTag", tag)
        .option("inferSchema", "true")
        .load(tempDir)

      println("\nSCHEMA INFERIDO:")
      println("=" * 100)

      xmlDF.printSchema()

      // =====================================================
      // COMPARAÇÃO
      // =====================================================

      val schemaInferido =
        flattenSchema(xmlDF.schema).toSet

      val schemaOficial =
        flattenSchema(
          BPeTASchema.createSchema()
        ).toSet

      val camposNaoMapeados =
        schemaInferido
          .diff(schemaOficial)
          .filterNot(_.toLowerCase.contains("signature"))

      println("\n" + "=" * 100)
      println("CAMPOS NÃO MAPEADOS")
      println("=" * 100)

      if (camposNaoMapeados.isEmpty) {

        println("✔ Nenhuma diferença encontrada.")

      } else {

        camposNaoMapeados
          .toSeq
          .sorted
          .foreach(println)
      }

      // =====================================================
      // CAMPOS DO SCHEMA OFICIAL NÃO ENCONTRADOS
      // =====================================================

      val camposAusentes =
        schemaOficial.diff(schemaInferido)

      println("\n" + "=" * 100)
      println("CAMPOS EXISTENTES NO SCHEMA OFICIAL MAS AUSENTES NA AMOSTRA")
      println("=" * 100)

      if (camposAusentes.isEmpty) {

        println("✔ Nenhum campo ausente.")

      } else {

        camposAusentes
          .toSeq
          .sorted
          .foreach(println)
      }

      // =====================================================
      // LIMPEZA
      // =====================================================

      println("\nRemovendo diretório temporário...")

      FileSystem
        .get(spark.sparkContext.hadoopConfiguration)
        .delete(new Path(tempDir), true)

      println("Processamento finalizado.")

    } finally {

      spark.stop()
    }
  }
}

// BPeTAMap.main(Array())