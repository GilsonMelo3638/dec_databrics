package Utils.Auditoria.AuditoriaLogger

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStreamReader}
import java.util.Date

object GeradorRelatorioAuditoria {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GeradorRelatorioAuditoria")
      .getOrCreate()

    try {

      val fs = FileSystem.get(
        spark.sparkContext.hadoopConfiguration
      )

      val logDir = new Path("/app/logs")

      if (!fs.exists(logDir)) {
        println(s"Diretório não encontrado: $logDir")
        return
      }

      val arquivos = fs.listStatus(logDir)
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

      spark.stop()

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

    try {

      var linha = reader.readLine()

      while (linha != null) {

        totalLinhas += 1

        if (linha.contains("Processando documento:"))
          documentosAuditados += 1

        if (linha.contains("ERRO"))
          errosExecucao += 1

        if (linha.contains("Total de CHAVEs não encontradas no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUs não encontrados no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUDFs não encontrados no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUSVDs não encontrados no prata:"))
          ausenciasEncontradas += extrairNumero(linha)

        if (linha.contains("Total de CHAVEs duplicadas:"))
          duplicidadesEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUs duplicados:"))
          duplicidadesEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUDFs duplicados:"))
          duplicidadesEncontradas += extrairNumero(linha)

        if (linha.contains("Total de NSUSVDs duplicados:"))
          duplicidadesEncontradas += extrairNumero(linha)

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

    println("========================================================")
    println("RELATÓRIO RESUMIDO DE AUDITORIA")
    println("========================================================")
    println()
    println(s"Data do relatório: ${new Date()}")
    println()
    println(s"Arquivo analisado: $arquivoLog")
    println()
    println("RESUMO")
    println()
    println(s"Documentos auditados: $documentosAuditados")
    println()
    println(s"Ausências encontradas: $ausenciasEncontradas")
    println()
    println(s"Duplicidades encontradas: $duplicidadesEncontradas")
    println()
    println(s"Erros de execução: $errosExecucao")
    println()
    println(s"Status: $status")
    println()
    println(s"Total de linhas analisadas: $totalLinhas")
    println()
    println("========================================================")
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

//GeradorRelatorioAuditoria.main(Array())