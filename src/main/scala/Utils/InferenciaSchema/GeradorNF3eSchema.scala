package Utils.InferenciaSchema

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs.{FileSystem, Path}

object GeradorNF3eSchema {

  // ============================================
  // 🔥 Converte StructType → código Scala
  // ============================================
  def structToScala(schema: StructType, indent: Int = 0): String = {

    val space = "  " * indent

    val fields = schema.fields.map { field =>

      val fieldCode = field.dataType match {

        case st: StructType =>
          s"""new StructType()\n${structToScala(st, indent + 2)}\n${space}"""

        case ArrayType(st: StructType, _) =>
          s"""ArrayType(\n${space}  new StructType()\n${structToScala(st, indent + 3)}\n${space}  , true)"""

        case ArrayType(dt, _) =>
          s"ArrayType(${simpleType(dt)}, true)"

        case _ =>
          simpleType(field.dataType)
      }

      s"""${space}.add("${field.name}", $fieldCode, true)"""
    }

    fields.mkString("\n")
  }

  def simpleType(dt: DataType): String = dt match {
    case StringType => "StringType"
    case DoubleType => "DoubleType"
    case IntegerType => "IntegerType"
    case LongType => "LongType"
    case BooleanType => "BooleanType"
    case TimestampType => "TimestampType"
    case DateType => "DateType"
    case _ => "StringType"
  }

  // ============================================
  // 🎯 MAIN
  // ============================================
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Gerador Automático NF3eSchema")
      .enableHiveSupport()
      .getOrCreate()

    try {

      val documento = "nf3e"
      val tag = "nf3eProc"
      val ano = "2026"
      val mesInicio = 3
      val mesFim = 4

      var dfAcumulado: DataFrame = null
      var totalGeral: Long = 0

      // ============================================
      // 🔁 LOOP COM PRINTS
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

        println(s"📊 Total linhas parquet: ${df.count()}")

        if (df.columns.contains("XML_DOCUMENTO_CLOB")) {

          val dfXml = df
            .select(col("XML_DOCUMENTO_CLOB"))
            .filter(col("XML_DOCUMENTO_CLOB").isNotNull)

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
          println("❌ Coluna XML_DOCUMENTO_CLOB não encontrada")
        }
      }

      // ============================================
      // 🔍 VALIDAÇÃO FINAL
      // ============================================

      println("\n" + "=" * 80)
      println(s"📦 TOTAL CONSOLIDADO DE XMLs: $totalGeral")
      println("=" * 80)

      if (dfAcumulado == null) {
        println("❌ Nenhum XML encontrado no período.")
        return
      }

      // ============================================
      // 🧱 TEMP
      // ============================================

      val tempDir =
        s"/tmp/schema_gen_${System.currentTimeMillis()}"

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

      println("\n==============================")
      println("📐 SCHEMA INFERIDO (CONSOLIDADO)")
      println("==============================")

      xmlDF.printSchema()

      // ============================================
      // 🧠 GERAÇÃO DO SCALA
      // ============================================

      println("\n⚙️ Gerando código Scala do schema...")

      val scalaSchema =
        s"""
package Schemas

import org.apache.spark.sql.types._

object NF3eSchemaAuto {
  def createSchema(): StructType = {
    new StructType()
${structToScala(xmlDF.schema, 2)}
  }
}
"""

      println("\n==============================")
      println("📄 SCHEMA GERADO (SCALA)")
      println("==============================")

      println(scalaSchema)

      // ============================================
      // 🧹 CLEAN
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

//GeradorNF3eSchema.main(Array())