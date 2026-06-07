package Utils.Auditoria.AuditoriaLogger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer

object GeradorRelatorioAuditoria {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GeradorRelatorioAuditoria")
      .getOrCreate()

    try {

      val fs =
        FileSystem.get(
          spark.sparkContext.hadoopConfiguration
        )

      val logDir =
        new Path("/app/logs")

      if (!fs.exists(logDir)) {
        println(s"Diretório não encontrado: $logDir")
        return
      }

      val arquivos =
        fs.listStatus(logDir)
          .filter(_.getPath.getName.startsWith("auditoria_"))

      if (arquivos.isEmpty) {
        println("Nenhum arquivo de auditoria encontrado.")
        return
      }

      val ultimoArquivo =
        arquivos.maxBy(_.getModificationTime)

      println(
        s"Último log encontrado: ${ultimoArquivo.getPath}"
      )

      gerarRelatorio(
        fs,
        ultimoArquivo.getPath.toString
      )

    } finally {

 //     spark.stop()

    }
  }

  def gerarRelatorio(
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

    var totalLinhas = 0

    var documentosAuditados = 0
    var ausenciasEncontradas = 0
    var duplicidadesEncontradas = 0
    var errosExecucao = 0

    var volumeBronzeNFE = ""
    var volumeBronzeNFCE = ""

    var volumeInfNFE = ""
    var volumeDetNFE = ""

    var volumeInfNFCE = ""
    var volumeDetNFCE = ""

    var inicioExecucao = ""
    var fimExecucao = ""

    var auditoriaNFE = false
    var auditoriaNFCE = false

    var diretoriosHDFSVerificados = 0
    var diretoriosHDFSOk = 0
    var diretoriosHDFSErro = 0
    var totalArquivosHDFS = 0

    var diretorioAtual = ""
    var ultimaPastaTemp = ""

    val resumoPastasHDFS =
      ArrayBuffer[(String, String, Int)]()

    try {

      var linha = reader.readLine()

      while (linha != null) {

        totalLinhas += 1

        if (inicioExecucao.isEmpty)
          inicioExecucao = extrairData(linha)

        fimExecucao = extrairData(linha)

        if (
          linha.contains("Processando documento:") ||
            linha.contains("Processando documento com NSU:") ||
            linha.contains("Processando documento com NSUDF:") ||
            linha.contains("Processando documento com NSUSVD:")
        ) {
          documentosAuditados += 1
        }

        if (
          linha.contains("ERRO") ||
            linha.contains("Exception")
        ) {
          errosExecucao += 1
        }

        if (linha.contains("Total de CHAVEs não encontradas no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUs não encontrados no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUDFs não encontrados no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUSVDs não encontrados no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Quantidade de chaves faltantes encontradas:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Duplicidade encontrada"))
          duplicidadesEncontradas += 1

        if (linha.contains("Quantidade registros bronze:")) {

          val valor = extrairNumeroLong(linha)

          if (auditoriaNFE && volumeBronzeNFE.isEmpty)
            volumeBronzeNFE = valor

          if (auditoriaNFCE && volumeBronzeNFCE.isEmpty)
            volumeBronzeNFCE = valor
        }

        if (linha.contains("Quantidade chaves INF:")) {

          val valor = extrairNumeroLong(linha)

          if (auditoriaNFE && volumeInfNFE.isEmpty)
            volumeInfNFE = valor

          if (auditoriaNFCE && volumeInfNFCE.isEmpty)
            volumeInfNFCE = valor
        }

        if (linha.contains("Quantidade registros DET:")) {

          val valor = extrairNumeroLong(linha)

          if (auditoriaNFE && volumeDetNFE.isEmpty)
            volumeDetNFE = valor

          if (auditoriaNFCE && volumeDetNFCE.isEmpty)
            volumeDetNFCE = valor
        }

        if (linha.contains("Iniciando processamento de NFE")) {
          auditoriaNFE = true
          auditoriaNFCE = false
        }

        if (linha.contains("Iniciando processamento de NFCE")) {
          auditoriaNFE = false
          auditoriaNFCE = true
        }

        if (linha.contains("Verificando diretório:")) {

          diretoriosHDFSVerificados += 1

          diretorioAtual =
            linha.split("Verificando diretório:")
              .last
              .trim
        }

        if (linha.contains("✅ OK:"))
          diretoriosHDFSOk += 1

        if (linha.contains("❌ ERRO:"))
          diretoriosHDFSErro += 1

        if (linha.contains("Última pasta:")) {

          ultimaPastaTemp =
            linha.split("Última pasta:")
              .last
              .trim
        }

        if (linha.contains("Quantidade de arquivos:")) {

          val qtd =
            extrairNumero(linha)

          totalArquivosHDFS += qtd

          if (
            diretorioAtual.nonEmpty &&
              ultimaPastaTemp.nonEmpty
          ) {

            val data =
              ultimaPastaTemp.split("/").last

            val nomeDiretorio =
              diretorioAtual
                .split("/")
                .takeRight(2)
                .mkString("_")

            resumoPastasHDFS +=
              ((nomeDiretorio, data, qtd))
          }
        }

        linha = reader.readLine()
      }

    } finally {

      reader.close()

    }

    val status =
      if (
        ausenciasEncontradas == 0 &&
          duplicidadesEncontradas == 0 &&
          errosExecucao == 0
      ) "SUCESSO"
      else "ATENÇÃO"

    println()
    println("============================================================")
    println("RELATÓRIO EXECUTIVO DE AUDITORIA")
    println("============================================================")
    println()

    println(s"Arquivo analisado : $arquivoLog")
    println(s"Início execução   : $inicioExecucao")
    println(s"Fim execução      : $fimExecucao")

    println()
    println("------------------------------------------------------------")
    println("STATUS GERAL")
    println("------------------------------------------------------------")
    println()

    println(s"Resultado final..............: $status")
    println(s"Documentos auditados.........: $documentosAuditados")
    println(s"Ausências encontradas........: $ausenciasEncontradas")
    println(s"Duplicidades encontradas.....: $duplicidadesEncontradas")
    println(s"Erros de execução............: $errosExecucao")

    println()
    println("------------------------------------------------------------")
    println("AUDITORIA NFE")
    println("------------------------------------------------------------")
    println()

    println(s"Volume Bronze...............: $volumeBronzeNFE")
    println(s"Volume INF..................: $volumeInfNFE")
    println(s"Volume DET..................: $volumeDetNFE")

    println()
    println("------------------------------------------------------------")
    println("AUDITORIA NFCE")
    println("------------------------------------------------------------")
    println()

    println(s"Volume Bronze...............: $volumeBronzeNFCE")
    println(s"Volume INF..................: $volumeInfNFCE")
    println(s"Volume DET..................: $volumeDetNFCE")

    println()
    println("------------------------------------------------------------")
    println("ÚLTIMAS PASTAS HDFS")
    println("------------------------------------------------------------")
    println()

    println(s"Diretórios verificados....: $diretoriosHDFSVerificados")
    println(s"Diretórios OK.............: $diretoriosHDFSOk")
    println(s"Diretórios com erro.......: $diretoriosHDFSErro")
    println(s"Total arquivos encontrados: $totalArquivosHDFS")

    if (resumoPastasHDFS.nonEmpty) {

      println()
      println("------------------------------------------------------------")
      println("RESUMO DAS ÚLTIMAS PASTAS HDFS")
      println("------------------------------------------------------------")
      println()

      resumoPastasHDFS.foreach {
        case (diretorio, data, qtd) =>
          println(
            f"${diretorio.padTo(35,'.')} $data ($qtd arquivos)"
          )
      }
    }

    println()
    println("------------------------------------------------------------")
    println("CONCLUSÃO")
    println("------------------------------------------------------------")
    println()

    if (status == "SUCESSO") {

      println("✓ Nenhuma ausência encontrada")
      println("✓ Nenhuma duplicidade encontrada")
      println("✓ Nenhum erro de execução")
      println("✓ Integridade validada com sucesso")

      if (diretoriosHDFSVerificados > 0) {
        println(s"✓ $diretoriosHDFSOk diretórios HDFS verificados")
        println(s"✓ $totalArquivosHDFS arquivos localizados")
      }

    } else {

      println("⚠ Foram encontradas inconsistências")
      println(s"Ausências....: $ausenciasEncontradas")
      println(s"Duplicidades.: $duplicidadesEncontradas")
      println(s"Erros........: $errosExecucao")
    }

    println()
    println(s"Total de linhas analisadas: $totalLinhas")
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

  private def extrairNumeroLong(
                                 linha: String
                               ): String = {

    try {

      linha
        .split(":")
        .last
        .trim

    } catch {

      case _: Exception => ""
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
// :load GeradorRelatorioAuditoria.scala
// GeradorRelatorioAuditoria.main(Array())
