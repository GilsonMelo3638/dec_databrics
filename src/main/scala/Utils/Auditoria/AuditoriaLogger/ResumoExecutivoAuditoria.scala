package Utils.Auditoria.AuditoriaLogger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ResumoExecutivoAuditoria {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ResumoExecutivoAuditoria")
      .getOrCreate()

    try {

      val fs =
        FileSystem.get(
          spark.sparkContext.hadoopConfiguration
        )

      val logDir =
        new Path("/app/logs")

      if (!fs.exists(logDir)) {
        println("Diretório de logs não encontrado.")
        return
      }

      val arquivos =
        fs.listStatus(logDir)
          .filter(_.getPath.getName.startsWith("auditoria_"))

      if (arquivos.isEmpty) {
        println("Nenhum log encontrado.")
        return
      }

      val ultimoArquivo =
        arquivos.maxBy(_.getModificationTime)

      gerarResumo(
        fs,
        ultimoArquivo.getPath.toString
      )

    } finally {

//      spark.stop()

    }

  }

  def gerarResumo(
                   fs: FileSystem,
                   arquivoLog: String
                 ): Unit = {

    val reader =
      new BufferedReader(
        new InputStreamReader(
          fs.open(new Path(arquivoLog)),
          "UTF-8"
        )
      )

    val erros =
      ArrayBuffer[String]()

    val alertas =
      ArrayBuffer[String]()

    val ausencias =
      ArrayBuffer[String]()

    val duplicidades =
      ArrayBuffer[String]()

    val problemasHDFS =
      ArrayBuffer[String]()

    var inicioExecucao = ""
    var fimExecucao = ""
    var alertasDataHDFS = ArrayBuffer[String]()

    try {

      var linha = reader.readLine()

      while (linha != null) {

        if (inicioExecucao.isEmpty)
          inicioExecucao = extrairData(linha)

        fimExecucao = extrairData(linha)

        // ERROS

        if (
          linha.contains("ERRO") ||
            linha.contains("Exception") ||
            linha.contains("Stack trace")
        ) {

          erros += linha.trim
        }

        // ALERTAS

        if (
          linha.contains("ALERTA") ||
            linha.contains("⚠️")
        ) {

          alertas += linha.trim
        }

        // AUSÊNCIAS

        if (
          linha.contains("não encontrados no prata") ||
            linha.contains("não encontradas no prata") ||
            linha.contains("chaves faltantes encontradas")
        ) {

          val valor =
            extrairNumero(linha)

          if (valor > 0)
            ausencias += linha.trim
        }

        // DUPLICIDADES

        if (
          linha.contains("Duplicidades encontradas:")
        ) {

          val valor =
            extrairNumero(linha)

          if (valor > 0)
            duplicidades += linha.trim
        }

        // HDFS

        if (
          linha.contains("A última pasta está VAZIA") ||
            linha.contains("Nenhuma pasta encontrada") ||
            linha.contains("ERRO ao processar diretório")
        ) {

          problemasHDFS += linha.trim
        }

        linha = reader.readLine()
      }

    } finally {

      reader.close()

    }

    val totalProblemas =
      erros.size +
        alertas.size +
        ausencias.size +
        duplicidades.size +
        problemasHDFS.size

    println()
    println("============================================================")
    println("RESUMO EXECUTIVO DA AUDITORIA")
    println("============================================================")
    println()

    println(s"Arquivo analisado : $arquivoLog")
    println(s"Início execução   : $inicioExecucao")
    println(s"Fim execução      : $fimExecucao")

    println()

    if (problemasHDFS.nonEmpty) {

      println("------------------------------------------------------------")
      println("PROBLEMAS HDFS")
      println("------------------------------------------------------------")

      problemasHDFS.distinct.foreach(println)

      println()
    }

    if (ausencias.nonEmpty) {

      println("------------------------------------------------------------")
      println("AUSÊNCIAS BRONZE x PRATA")
      println("------------------------------------------------------------")

      ausencias.distinct.foreach(println)

      println()
    }

    if (duplicidades.nonEmpty) {

      println("------------------------------------------------------------")
      println("DUPLICIDADES")
      println("------------------------------------------------------------")

      duplicidades.distinct.foreach(println)

      println()
    }

    if (alertas.nonEmpty) {

      println("------------------------------------------------------------")
      println("ALERTAS")
      println("------------------------------------------------------------")

      alertas.distinct.foreach(println)

      println()
    }

    if (erros.nonEmpty) {

      println("------------------------------------------------------------")
      println("ERROS")
      println("------------------------------------------------------------")

      erros.distinct.foreach(println)

      println()
    }

    println("------------------------------------------------------------")
    println("RESUMO FINAL")
    println("------------------------------------------------------------")
    println()

    println(s"Problemas HDFS........: ${problemasHDFS.size}")
    println(s"Ausências............: ${ausencias.size}")
    println(s"Duplicidades.........: ${duplicidades.size}")
    println(s"Alertas..............: ${alertas.size}")
    println(s"Erros................: ${erros.size}")

    println()

    if (totalProblemas == 0) {

      println("STATUS FINAL: SUCESSO")
      println()
      println("✓ Nenhuma inconsistência encontrada")
      println("✓ Nenhuma ausência bronze x prata")
      println("✓ Nenhuma duplicidade")
      println("✓ Nenhum erro de execução")
      println("✓ Todos os diretórios HDFS verificados")

    } else {

      println("STATUS FINAL: ATENÇÃO")
      println()
      println(
        s"Foram encontradas $totalProblemas ocorrências que exigem análise."
      )
    }

    println()
    println("============================================================")
  }

  private def extrairNumero(
                             linha: String
                           ): Int = {

    try {

      linha
        .replaceAll("[^0-9]", "")
        .toInt

    } catch {

      case _: Exception => 0
    }

  }

  private def extrairData(
                           linha: String
                         ): String = {

    try {

      val regex =
        "\\[(.*?)\\]".r

      regex.findFirstMatchIn(linha)
        .map(_.group(1))
        .getOrElse("")

    } catch {

      case _: Exception => ""
    }

  }
}

// Executar:
//
// :load ResumoExecutivoAuditoria.scala
// ResumoExecutivoAuditoria.main(Array())
