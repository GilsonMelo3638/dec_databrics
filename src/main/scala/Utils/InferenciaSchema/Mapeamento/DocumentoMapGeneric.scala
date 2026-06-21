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
  class HdfsLogger(spark: SparkSession, logDir: String, filePrefix: String = "log") {
    private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    private val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    private val logFileName = s"$logDir/${filePrefix}_${timestamp}.txt"
    private val logPath = new Path(logFileName)
    private var writer: BufferedWriter = _

    fs.mkdirs(new Path(logDir))
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(logPath, true)))

    def log(message: String): Unit = {
      println(message)
      writer.write(message)
      writer.newLine()
      writer.flush()
    }

    def close(): Unit = {
      if (writer != null) writer.close()
    }
  }

  // ============================================
  // 📦 RESULTADO CONSOLIDADO DE CADA DOCUMENTO
  // ============================================
  case class ResultadoInferencia(
                                  grupo: String,
                                  documento: String,
                                  nomeLog: String,
                                  tag: String,
                                  periodo: String,
                                  totalXmls: Long,
                                  camposSchemaOficial: Int,
                                  camposSchemaInferido: Int,
                                  camposNaoMapeados: Seq[String],
                                  schemaCompleto: Boolean,
                                  schemaInferidoAdd: String
                                )

  // ============================================
  // 🔍 FLATTEN DE SCHEMA
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
  // 🔧 CONVERSÃO DE TIPOS SPARK -> TEXTO SCALA
  // ============================================
  def sparkType(dt: DataType): String = dt match {
    case StringType        => "StringType"
    case LongType          => "LongType"
    case IntegerType       => "IntegerType"
    case DoubleType        => "DoubleType"
    case FloatType         => "FloatType"
    case BooleanType       => "BooleanType"
    case DateType          => "DateType"
    case TimestampType     => "TimestampType"
    case ShortType         => "ShortType"
    case ByteType          => "ByteType"
    case BinaryType        => "BinaryType"
    case DecimalType()     => "DecimalType.SYSTEM_DEFAULT"
    case NullType          => "NullType"
    case _                 => s"${dt.typeName.capitalize}Type"
  }

  // ============================================
  // 🧱 GERA BLOCO .add(...) A PARTIR DO SCHEMA
  // ============================================
  def toAddSchema(schema: StructType, indent: Int = 0): String = {
    val tab = "  " * indent

    schema.fields.map { field =>
      field.dataType match {

        case s: StructType =>
          s"""${tab}.add("${field.name}",
${tab}  new StructType()
${toAddSchema(s, indent + 2)}
${tab}  )"""

        case ArrayType(s: StructType, containsNull) =>
          s"""${tab}.add("${field.name}",
${tab}  ArrayType(
${tab}    new StructType()
${toAddSchema(s, indent + 3)}
${tab}    ),
${tab}    $containsNull
${tab}  )
${tab})"""

        case ArrayType(t, containsNull) =>
          s"""${tab}.add("${field.name}",
${tab}  ArrayType(${sparkType(t)}, $containsNull)
${tab})"""

        case t =>
          s"""${tab}.add("${field.name}", ${sparkType(t)}, true)"""
      }
    }.mkString("\n")
  }

  // ============================================
  // 🗂️ DEFINE O GRUPO FUNCIONAL
  // ============================================
  def grupoDocumento(documento: String, nomeLog: String): String = {
    val d = documento.toLowerCase
    val n = nomeLog.toLowerCase

    if (d == "cte") "CTE"
    else if (d == "mdfe") "MDFE"
    else if (d == "bpe") "BPE"
    else if (d == "nf3e") "NF3E"
    else if (d == "nfcom") "NFCOM"
    else if (d == "nfe") "NFE"
    else if (d == "nfce") "NFCE"
    else s"${d}_${n}".toUpperCase
  }

  // ============================================
  // 📝 RELATÓRIO FINAL CONSOLIDADO
  // ============================================
  def gerarRelatorioFinal(
                           spark: SparkSession,
                           resultados: Seq[ResultadoInferencia],
                           logDir: String
                         ): Unit = {

    val logger = new HdfsLogger(
      spark,
      logDir,
      filePrefix = "relatorio_inferencia_schema"
    )

    try {
      val completos = resultados.filter(_.schemaCompleto)
      val incompletos = resultados.filter(!_.schemaCompleto)

      logger.log("======================================================================")
      logger.log("RELATÓRIO FINAL CONSOLIDADO - INFERÊNCIA DE SCHEMA")
      logger.log("======================================================================")
      logger.log(s"Data/Hora: ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}")
      logger.log(s"Total de documentos processados: ${resultados.size}")
      logger.log(s"Schemas completos: ${completos.size}")
      logger.log(s"Schemas incompletos: ${incompletos.size}")
      logger.log("")

      // ============================================================
      // RESUMO GERAL
      // ============================================================
      logger.log("######################################################################")
      logger.log("RESUMO GERAL")
      logger.log("######################################################################")

      resultados
        .sortBy(r => (r.grupo, r.documento, r.nomeLog))
        .foreach { r =>
          logger.log(
            f"${r.grupo}%-8s | ${r.documento}%-6s | ${r.nomeLog}%-10s | XMLs=${r.totalXmls}%8d | Oficial=${r.camposSchemaOficial}%4d | Inferido=${r.camposSchemaInferido}%4d | Não mapeados=${r.camposNaoMapeados.size}%4d | ${if (r.schemaCompleto) "COMPLETO" else "INCOMPLETO"}"
          )
        }

      logger.log("")

      // ============================================================
      // SCHEMAS COMPLETOS POR GRUPO
      // ============================================================
      logger.log("######################################################################")
      logger.log("SCHEMAS COMPLETOS")
      logger.log("######################################################################")

      if (completos.isEmpty) {
        logger.log("Nenhum schema completo encontrado.")
      } else {
        completos
          .groupBy(_.grupo)
          .toSeq
          .sortBy(_._1)
          .foreach { case (grupo, regs) =>
            logger.log("")
            logger.log(s"==================== GRUPO: $grupo ====================")

            regs.sortBy(r => (r.documento, r.nomeLog)).foreach { r =>
              logger.log(
                s"""- Documento: ${r.documento}
                   |  Nome log: ${r.nomeLog}
                   |  Tag: ${r.tag}
                   |  Período: ${r.periodo}
                   |  Total XMLs: ${r.totalXmls}
                   |  Campos schema oficial: ${r.camposSchemaOficial}
                   |  Campos schema inferido: ${r.camposSchemaInferido}
                   |  Status: COMPLETO
                   |""".stripMargin
              )
            }
          }
      }

      logger.log("")

      // ============================================================
      // SCHEMAS INCOMPLETOS POR GRUPO
      // ============================================================
      logger.log("######################################################################")
      logger.log("SCHEMAS INCOMPLETOS")
      logger.log("######################################################################")

      if (incompletos.isEmpty) {
        logger.log("Nenhum schema incompleto encontrado.")
      } else {
        incompletos
          .groupBy(_.grupo)
          .toSeq
          .sortBy(_._1)
          .foreach { case (grupo, regs) =>
            logger.log("")
            logger.log(s"==================== GRUPO: $grupo ====================")

            regs.sortBy(r => (r.documento, r.nomeLog)).foreach { r =>
              logger.log(
                s"""- Documento: ${r.documento}
                   |  Nome log: ${r.nomeLog}
                   |  Tag: ${r.tag}
                   |  Período: ${r.periodo}
                   |  Total XMLs: ${r.totalXmls}
                   |  Campos schema oficial: ${r.camposSchemaOficial}
                   |  Campos schema inferido: ${r.camposSchemaInferido}
                   |  Total campos não mapeados: ${r.camposNaoMapeados.size}
                   |  Status: INCOMPLETO
                   |  Campos não mapeados:
                   |${r.camposNaoMapeados.sorted.map(c => s"    - $c").mkString("\n")}
                   |
                   |  Schema inferido (.add):
                   |${r.schemaInferidoAdd.linesIterator.map("    " + _).mkString("\n")}
                   |""".stripMargin
              )
            }
          }
      }

      logger.log("")
      logger.log("======================================================================")
      logger.log("FIM DO RELATÓRIO FINAL CONSOLIDADO")
      logger.log("======================================================================")

    } finally {
      logger.close()
    }
  }

  // ============================================
  // ⚙️ EXECUTOR GENÉRICO COM LOG EM HDFS
  // ============================================
  def executar(
                spark: SparkSession,
                documento: String,
                nomeLog: String,
                tag: String,
                schemaOficial: StructType,
                ano: String,
                mesInicio: Int,
                mesFim: Int,
                aplicarFiltro: DataFrame => DataFrame,
                filtroCampos: String => Boolean
              ): ResultadoInferencia = {

    val logDir = s"/app/inferencia_schema/$nomeLog"
    val logger = new HdfsLogger(spark, logDir)
    val periodo = s"$ano-${f"$mesInicio%02d"} até $ano-${f"$mesFim%02d"}"
    val grupo = grupoDocumento(documento, nomeLog)

    try {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      var dfAcumulado: DataFrame = null
      var totalGeral: Long = 0

      logger.log(s"\n${"=" * 80}")
      logger.log(s"🚀 PROCESSANDO DOCUMENTO: $documento")
      logger.log(s"🗂️ GRUPO: $grupo")
      logger.log(s"🏷️ TAG: $tag")
      logger.log(s"${"=" * 80}")

      for (mesInt <- mesInicio to mesFim) {
        val mes = f"$mesInt%02d"
        val path = s"/datalake/bronze/sources/dbms/dec/diario/$documento/year=$ano/month=$mes/day=*"

        logger.log(s"\n📅 $documento | $ano-$mes")
        logger.log(s"📂 Path: $path")

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

        return ResultadoInferencia(
          grupo = grupo,
          documento = documento,
          nomeLog = nomeLog,
          tag = tag,
          periodo = periodo,
          totalXmls = 0L,
          camposSchemaOficial = flattenSchema(schemaOficial).size,
          camposSchemaInferido = 0,
          camposNaoMapeados = Seq("Nenhum XML encontrado para inferência"),
          schemaCompleto = false,
          schemaInferidoAdd = "Schema não gerado porque nenhum XML válido foi encontrado."
        )
      }

      logger.log(s"\n📦 TOTAL CONSOLIDADO: $totalGeral")

      val tempDir = s"/tmp/${documento}_${nomeLog}_xml_${System.currentTimeMillis()}"

      try {
        dfAcumulado.write.mode("overwrite").text(tempDir)

        val xmlDF = spark.read
          .format("xml")
          .option("rowTag", tag)
          .option("inferSchema", "true")
          .load(tempDir)

        val schemaInferidoAdd =
          s"""new StructType()
${toAddSchema(xmlDF.schema, 1)}
"""

        logger.log("\n📐 SCHEMA INFERIDO (.add)")
        logger.log("=" * 80)
        logger.log(schemaInferidoAdd)

        val schemaInferido = flattenSchema(xmlDF.schema).toSet
        val schemaOficialFlat = flattenSchema(schemaOficial).toSet

        val diff = schemaInferido
          .diff(schemaOficialFlat)
          .filter(filtroCampos)
          .toSeq
          .sorted

        logger.log("\n🔎 CAMPOS NÃO MAPEADOS")
        logger.log("=" * 80)

        if (diff.isEmpty) {
          logger.log("✔ Nenhuma diferença encontrada - Schema está completo!")
        } else {
          logger.log(s"⚠️ Total de campos não mapeados: ${diff.size}")
          logger.log("Lista de campos:")
          diff.foreach(campo => logger.log(s"  - $campo"))
        }

        logger.log("\n📊 RESUMO FINAL")
        logger.log("=" * 80)
        logger.log(s"  - Grupo: $grupo")
        logger.log(s"  - Documento: $documento")
        logger.log(s"  - Nome log: $nomeLog")
        logger.log(s"  - Tag: $tag")
        logger.log(s"  - Período: $periodo")
        logger.log(s"  - Total XMLs processados: $totalGeral")
        logger.log(s"  - Campos totais no schema oficial: ${schemaOficialFlat.size}")
        logger.log(s"  - Campos totais no schema inferido: ${schemaInferido.size}")
        logger.log(s"  - Campos não mapeados (após filtro): ${diff.size}")
        logger.log(s"  - Status: ${if (diff.isEmpty) "COMPLETO" else "INCOMPLETO"}")

        logger.log("\n✅ Processamento finalizado com sucesso!")
        logger.log("=" * 80)

        ResultadoInferencia(
          grupo = grupo,
          documento = documento,
          nomeLog = nomeLog,
          tag = tag,
          periodo = periodo,
          totalXmls = totalGeral,
          camposSchemaOficial = schemaOficialFlat.size,
          camposSchemaInferido = schemaInferido.size,
          camposNaoMapeados = diff,
          schemaCompleto = diff.isEmpty,
          schemaInferidoAdd = schemaInferidoAdd
        )

      } finally {
        fs.delete(new Path(tempDir), true)
      }

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

    val globalLogger = new HdfsLogger(
      spark,
      "/app/inferencia_schema/logs/global",
      filePrefix = "global"
    )

    try {
      val ano = "2026"
      val mesInicio = 5
      val mesFim = 6

      globalLogger.log(
        s"🎬 INÍCIO DO PROCESSAMENTO - ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}"
      )
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
      // EXECUÇÕES
      // =========================
      val resultados = Seq(

        // =========================
        // CTE
        // =========================
        executar(
          spark, "cte", "cte", "cteProc",
          CTeSchema.createSchema(),
          ano, mesInicio, mesFim,
          df =>
            df.filter(col("MODELO").cast("string") === "57")
              .filter(col("XML_DOCUMENTO_CLOB").rlike("<cteProc"))
              .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
              .select(col("XML_DOCUMENTO_CLOB")),
          ignoraSignature
        ),

        // =========================
        // CTE SIMP
        // =========================
        executar(
          spark, "cte", "ctesimp", "cteSimpProc",
          CTeSimpSchema.createSchema(),
          ano, mesInicio, mesFim,
          df =>
            df.filter(col("MODELO").cast("string") === "57")
              .filter(col("XML_DOCUMENTO_CLOB").rlike("<cteSimpProc"))
              .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
              .select(col("XML_DOCUMENTO_CLOB")),
          ignoraSignature
        ),

        // =========================
        // CTE OS
        // =========================
        executar(
          spark, "cte", "cteos", "cteOSProc",
          CTeOSSchema.createSchema(),
          ano, mesInicio, mesFim,
          df =>
            df.filter(col("MODELO").cast("string") === "67")
              .filter(col("XML_DOCUMENTO_CLOB").rlike("<cteOSProc"))
              .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
              .select(col("XML_DOCUMENTO_CLOB")),
          ignoraSignature
        ),

        // =========================
        // GTVe
        // =========================
        executar(
          spark, "cte", "gvte", "GTVeProc",
          GVTeSchema.createSchema(),
          ano, mesInicio, mesFim,
          df =>
            df.filter(col("MODELO").cast("string") === "64")
              .filter(col("XML_DOCUMENTO_CLOB").rlike("<GTVeProc"))
              .filter(col("XML_DOCUMENTO_CLOB").isNotNull)
              .select(col("XML_DOCUMENTO_CLOB")),
          ignoraSignature
        ),

        // =========================
        // MDFE
        // =========================
        executar(
          spark,
          "mdfe",
          "mdfe",
          "mdfeProc",
          MDFeSchema.createSchema(),
          ano,
          mesInicio,
          mesFim,
          filtroPadrao,
          ignoraSignature
        ),

        // =========================
        // BPE
        // =========================
        executar(
          spark,
          "bpe",
          "bpe",
          "bpeProc",
          BPeSchema.createSchema(),
          ano,
          mesInicio,
          mesFim,
          df =>
            df.filter(col("XML_DOCUMENTO_CLOB").isNotNull)
              .filter(col("XML_DOCUMENTO_CLOB").rlike("<BPe[\\s>]"))
              .select(col("XML_DOCUMENTO_CLOB")),
          ignoraSignature
        ),

        // =========================
        // BPETA
        // =========================
        executar(
          spark,
          "bpe",
          "bpeta",
          "bpeProc",
          BPeTASchema.createSchema(),
          ano,
          mesInicio,
          mesFim,
          df =>
            df.filter(col("XML_DOCUMENTO_CLOB").isNotNull)
              .filter(col("XML_DOCUMENTO_CLOB").rlike("<BPeTA[\\s>]"))
              .select(col("XML_DOCUMENTO_CLOB")),
          ignoraSignature
        ),

        // =========================
        // NF3E
        // =========================
        executar(
          spark,
          "nf3e",
          "nf3e",
          "nf3eProc",
          NF3eSchema.createSchema(),
          ano,
          mesInicio,
          mesFim,
          filtroPadrao,
          ignoraSignature
        ),

        // =========================
        // NFCOM
        // =========================
        executar(
          spark,
          "nfcom",
          "nfcom",
          "nfcomProc",
          NFComSchema.createSchema(),
          ano,
          mesInicio,
          mesFim,
          filtroPadrao,
          ignoraSignature
        ),

        // =========================
        // NFE - INF
        // =========================
        executar(
          spark, "nfe", "infnfe", "nfeProc",
          NFeSchema.createSchema(),
          ano, mesInicio, mesFim,
          filtroPadrao,
          campo => {
            val l = campo.toLowerCase
            !l.contains("signature") && !l.split("\\.").contains("det")
          }
        ),

        // =========================
        // NFE - DET
        // =========================
        executar(
          spark, "nfe", "detnfe", "nfeProc",
          NFeDetSchema.createSchema(),
          ano, mesInicio, mesFim,
          filtroPadrao,
          campo => {
            val partes = campo.toLowerCase.split("\\.")
            partes.contains("det") &&
              !partes.contains("signature") &&
              !partes.contains("infnfe")
          }
        ),

        // =========================
        // NFCE - INF
        // =========================
        executar(
          spark, "nfce", "infnfce", "nfeProc",
          NFCeSchema.createSchema(),
          ano, mesInicio, mesFim,
          filtroPadrao,
          campo => {
            val l = campo.toLowerCase
            !l.contains("signature") && !l.split("\\.").contains("det")
          }
        ),

        // =========================
        // NFCE - DET
        // =========================
        executar(
          spark, "nfce", "detnfce", "nfeProc",
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
      )

      // =========================
      // RELATÓRIO FINAL CONSOLIDADO
      // =========================
      gerarRelatorioFinal(
        spark,
        resultados,
        "/app/inferencia_schema/logs/relatorio_final"
      )

      globalLogger.log("")
      globalLogger.log("🎉 PROCESSAMENTO COMPLETO!")
      globalLogger.log(
        s"🏁 Término: ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}"
      )

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

// DocumentoMapGeneric.main(Array())