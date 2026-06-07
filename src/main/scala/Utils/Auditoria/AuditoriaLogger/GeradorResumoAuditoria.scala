package Utils.Auditoria.AuditoriaLogger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStreamReader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

object GeradorResumoAuditoria {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder()
        .appName("GeradorResumoAuditoria")
        .getOrCreate()

    try {

      val fs =
        FileSystem.get(
          spark.sparkContext.hadoopConfiguration
        )

      val logDir =
        new Path("/app/logs")

      if (!fs.exists(logDir)) {

        println("Diretório /app/logs não encontrado")
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

    val diretoriosAtrasados =
      ArrayBuffer[String]()

    val diretoriosVazios =
      ArrayBuffer[String]()

    var ausencias = 0
    var duplicidades = 0

    var diretorioAtual = ""

    val ontem =
      LocalDate.now().minusDays(1)

    val formatter =
      DateTimeFormatter.ofPattern("yyyyMMdd")

    try {

      var linha =
        reader.readLine()

      while (linha != null) {

        //
        // Diretório atual
        //
        if (
          linha.contains(
            "Verificando diretório:"
          )
        ) {

          diretorioAtual =
            linha.split(
              "Verificando diretório:"
            ).last.trim
        }

        //
        // Última pasta
        //
        if (
          linha.contains(
            "Última pasta encontrada em"
          )
        ) {

          try {

            val ultimaPasta =
              linha.split(":").last.trim

            val dataTexto =
              ultimaPasta.split("/").last

            if (
              dataTexto.matches("\\d{8}")
            ) {

              val dataPasta =
                LocalDate.parse(
                  dataTexto,
                  formatter
                )

              if (dataPasta != ontem) {

                diretoriosAtrasados +=
                  s"""
                     |Diretório:
                     |$diretorioAtual
                     |
                     |Última pasta:
                     |$dataTexto
                     |
                     |Esperado:
                     |${ontem.format(formatter)}
                     |""".stripMargin
              }
            }

          } catch {

            case _: Exception =>
          }
        }

        //
        // Diretório vazio
        //
        if (
          linha.contains(
            "A última pasta está VAZIA"
          )
        ) {

          diretoriosVazios += diretorioAtual
        }

        //
        // Ausências
        //
        if (
          linha.contains(
            "Total de CHAVEs não encontradas no prata:"
          )
        ) {

          ausencias += extrairNumero(linha)
        }

        if (
          linha.contains(
            "Total de NSUs não encontrados no prata:"
          )
        ) {

          ausencias += extrairNumero(linha)
        }

        if (
          linha.contains(
            "Total de NSUDFs não encontrados no prata:"
          )
        ) {

          ausencias += extrairNumero(linha)
        }

        if (
          linha.contains(
            "Total de NSUSVDs não encontrados no prata:"
          )
        ) {

          ausencias += extrairNumero(linha)
        }

        if (
          linha.contains(
            "Quantidade de chaves faltantes encontradas:"
          )
        ) {

          ausencias += extrairNumero(linha)
        }

        //
        // Duplicidades
        //
        if (
          linha.contains(
            "Duplicidades encontradas:"
          )
        ) {

          duplicidades += extrairNumero(linha)
        }

        //
        // Erros
        //
        if (
          linha.contains("❌ ERRO") ||
            linha.contains("Exception")
        ) {

          erros += linha
        }

        linha =
          reader.readLine()
      }

    } finally {

      reader.close()

    }

    val totalProblemas =
      diretoriosAtrasados.size +
        diretoriosVazios.size +
        ausencias +
        duplicidades +
        erros.size

    val status =
      if (totalProblemas == 0)
        "SUCESSO"
      else
        "ATENÇÃO"

    println()
    println("============================================================")
    println("RESUMO EXECUTIVO DE AUDITORIA")
    println("============================================================")
    println()

    println(s"Arquivo analisado: $arquivoLog")
    println(s"Status geral.....: $status")

    //
    // HDFS ATRASADO
    //
    if (diretoriosAtrasados.nonEmpty) {

      println()
      println("------------------------------------------------------------")
      println("DIRETÓRIOS HDFS ATRASADOS")
      println("------------------------------------------------------------")

      diretoriosAtrasados.foreach { alerta =>

        println()
        println(alerta)
      }
    }

    //
    // HDFS VAZIO
    //
    if (diretoriosVazios.nonEmpty) {

      println()
      println("------------------------------------------------------------")
      println("DIRETÓRIOS HDFS VAZIOS")
      println("------------------------------------------------------------")

      diretoriosVazios.foreach { dir =>

        println()
        println(dir)
      }
    }

    //
    // AUDITORIA
    //
    if (
      ausencias > 0 ||
        duplicidades > 0
    ) {

      println()
      println("------------------------------------------------------------")
      println("INCONSISTÊNCIAS DE AUDITORIA")
      println("------------------------------------------------------------")

      if (ausencias > 0)
        println(s"Ausências encontradas : $ausencias")

      if (duplicidades > 0)
        println(s"Duplicidades encontradas : $duplicidades")
    }

    //
    // ERROS
    //
    if (erros.nonEmpty) {

      println()
      println("------------------------------------------------------------")
      println("ERROS")
      println("------------------------------------------------------------")

      erros.distinct.foreach(println)
    }

    //
    // RESUMO FINAL
    //
    println()
    println("------------------------------------------------------------")
    println("RESUMO FINAL")
    println("------------------------------------------------------------")

    println(s"Diretórios atrasados : ${diretoriosAtrasados.size}")
    println(s"Diretórios vazios    : ${diretoriosVazios.size}")
    println(s"Ausências            : $ausencias")
    println(s"Duplicidades         : $duplicidades")
    println(s"Erros                : ${erros.size}")

    println()
    println(s"STATUS FINAL: $status")

    println()
    println("============================================================")
  }

  private def extrairNumero(
                             linha: String
                           ): Int = {

    try {

      linha
        .split(":")
        .last
        .trim
        .replaceAll("[^0-9]", "")
        .toInt

    } catch {

      case _: Exception => 0
    }
  }
}

//GeradorResumoAuditoria.main(Array())