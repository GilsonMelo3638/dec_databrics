package Utils.InferenciaSchema.Mapeamento

import Schemas._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DocumentoMapGeneric {

  // ============================================
  // 📝 UTILITÁRIO PARA LOG EM HDFS
  // ============================================
  class HdfsLogger(spark: SparkSession, logDir: String) {
    private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    private val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    private val logFileName = s"${logDir}/log_${timestamp}.txt"
    private val logPath = new Path(logFileName)
    private var writer: BufferedWriter = _

    // Garantir que o diretório existe
    fs.mkdirs(new Path(logDir))

    // Inicializar writer
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(logPath, true)))

    def log(message: String): Unit = {
      println(message)
      writer.write(message)
      writer.newLine()
      writer.flush()
    }

    def close(): Unit = {
      if (writer != null) {
        writer.close()
      }
    }
  }

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
  // ⚙️ EXECUTOR GENÉRICO COM LOG EM HDFS
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

    // Criar logger específico para este documento
    val logDir = s"/app/inferencia_schema/${documento}"
    val logger = new HdfsLogger(spark, logDir)

    try {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      var dfAcumulado: DataFrame = null
      var totalGeral: Long = 0

      logger.log(s"\n${"=" * 60}")
      logger.log(s"🚀 PROCESSANDO DOCUMENTO: $documento")
      logger.log(s"🏷️ TAG: $tag")
      logger.log(s"${"=" * 60}")

      for (mesInt <- mesInicio to mesFim) {

        val mes = f"$mesInt%02d"

        val path =
          s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

        logger.log(s"\n📅 $documento | $ano-$mes")
        logger.log(s"📂 Path: $path")

        // 🔥 valida path
        if (!fs.globStatus(new Path(path)).nonEmpty) {
          logger.log("⚠️ Path não encontrado")
        } else {

          val df = spark.read.parquet(path)

          if (df.columns.contains("XML_DOCUMENTO_CLOB")) {

            val dfFiltrado = aplicarFiltro(df).cache()

            val totalMes = dfFiltrado.count()
            logger.log(s"📄 XMLs válidos: $totalMes")

            if (totalMes > 0) {

              totalGeral += totalMes

              dfAcumulado =
                if (dfAcumulado == null) dfFiltrado
                else dfAcumulado.unionByName(dfFiltrado)
            }

          } else {
            logger.log("❌ Coluna XML_DOCUMENTO_CLOB não encontrada")
          }
        }
      }

      if (dfAcumulado == null) {
        logger.log("❌ Nenhum XML encontrado.")
        return
      }

      logger.log(s"\n📦 TOTAL CONSOLIDADO: $totalGeral")

      val tempDir =
        s"/tmp/${documento}_xml_${System.currentTimeMillis()}"

      dfAcumulado.write.mode("overwrite").text(tempDir)

      val xmlDF = spark.read
        .format("xml")
        .option("rowTag", tag)
        .option("inferSchema", "true")
        .load(tempDir)

      logger.log("\n📐 SCHEMA INFERIDO:")

      // Capturar schema inferido
      val schemaString = new StringBuilder()
      xmlDF.schema.foreach { field =>
        schemaString.append(s"  ${field.name}: ${field.dataType.simpleString}\n")
      }
      logger.log(schemaString.toString())

      // Alternativamente, usar printSchema mas redirecionar para log
      val originalOut = System.out
      val baos = new java.io.ByteArrayOutputStream()
      val printStream = new java.io.PrintStream(baos)
      System.setOut(printStream)
      xmlDF.printSchema()
      System.setOut(originalOut)
      logger.log(baos.toString())

      val schemaInferido = flattenSchema(xmlDF.schema).toSet
      val schemaOficialFlat = flattenSchema(schemaOficial).toSet

      val diff = schemaInferido
        .diff(schemaOficialFlat)
        .filter(filtroCampos)

      logger.log("\n🔎 CAMPOS NÃO MAPEADOS")
      logger.log("=" * 50)

      if (diff.isEmpty) {
        logger.log("✔ Nenhuma diferença encontrada - Schema está completo!")
      } else {
        logger.log(s"⚠️ Total de campos não mapeados: ${diff.size}")
        logger.log("Lista de campos:")
        diff.toSeq.sorted.foreach(campo => logger.log(s"  - $campo"))
      }

      logger.log("\n📊 RESUMO FINAL:")
      logger.log(s"  - Documento: $documento")
      logger.log(s"  - Período: $ano-${f"$mesInicio%02d"} até $ano-${f"$mesFim%02d"}")
      logger.log(s"  - Total XMLs processados: $totalGeral")
      logger.log(s"  - Campos totais no schema oficial: ${schemaOficialFlat.size}")
      logger.log(s"  - Campos totais no schema inferido: ${schemaInferido.size}")
      logger.log(s"  - Campos não mapeados (após filtro): ${diff.size}")

      fs.delete(new Path(tempDir), true)

      logger.log("\n✅ Processamento finalizado com sucesso!")
      logger.log("=" * 60)

    } finally {
      logger.close()
    }
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

    // Logger global para o processo principal
    val globalLogger = new HdfsLogger(spark, "/datalake/logs/inferencia_schema/global")

    try {
      import spark.implicits._

      val ano = "2026"
      val mesInicio = 5
      val mesFim = 6

      globalLogger.log(s"🎬 INÍCIO DO PROCESSAMENTO - ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}")
      globalLogger.log(s"📆 Período: $ano - Mês $mesInicio até $mesFim")
      globalLogger.log("")

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

      globalLogger.log("\n🎉 PROCESSAMENTO COMPLETO!")
      globalLogger.log(s"🏁 Término: ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}")

    } catch {
      case e: Exception =>
        globalLogger.log(s"❌ ERRO FATAL: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      globalLogger.close()
      spark.stop()
    }
  }
}

//DocumentoMapGeneric.main(Array())