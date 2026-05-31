package Utils.Auditoria.AuditoriaLogger

import scala.sys.process._

object UltimaPastaHDFS {

  /**
   * Método principal utilizado quando executado standalone.
   * Mantém compatibilidade com execução direta:
   *
   * UltimaPastaHDFS.main(Array())
   */
  def main(args: Array[String]): Unit = {
    executar()
  }

  /**
   * Método reutilizável que aceita função de log.
   *
   * Por padrão usa println.
   * Quando chamado pelo AuditoriaLogger:
   *
   * UltimaPastaHDFS.executar(HDFSLogger.log)
   *
   * os logs também serão gravados em arquivo HDFS.
   */
  def executar(logFn: String => Unit = println): Unit = {

    val diretorios = Seq(
      "/datalake/bronze/sources/dbms/dec/processamento/nfce/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfe/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/bpe/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/mdfe/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nf3e/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/cte/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfcom/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfce_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/bpe_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nf3e_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfe_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/mdfe_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/cte_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfcom_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfe_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/cte_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/bpe_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/mdfe_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nf3e_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfcom_evento/processado"
    )

    logFn(s"Iniciando verificação de ${diretorios.size} diretórios HDFS")

    diretorios.foreach { dir =>

      try {

        logFn(s"Verificando diretório: $dir")

        val comandoListarPastas = s"hdfs dfs -ls $dir"
        val resultado = comandoListarPastas.!!

        val pastas = resultado
          .split("\n")
          .filter(_.startsWith("d")) // apenas diretórios
          .map(_.trim.split("\\s+").last)
          .sorted
          .reverse

        pastas.headOption match {

          case Some(ultimaPasta) =>

            logFn(s"Última pasta encontrada em $dir: $ultimaPasta")

            // Verifica arquivos dentro da última pasta
            val comandoListarArquivos = s"hdfs dfs -ls $ultimaPasta"
            val conteudo = comandoListarArquivos.!!

            val arquivos = conteudo
              .split("\n")
              .filter(_.startsWith("-")) // apenas arquivos

            if (arquivos.isEmpty) {

              logFn(
                s"""
                   |🚨 ALERTA:
                   |A última pasta está VAZIA!
                   |Diretório base: $dir
                   |Última pasta: $ultimaPasta
                   |Possível falha de processamento ou ingestão.
                   |""".stripMargin
              )

              // Caso queira falhar o job:
              // sys.error(s"Última pasta vazia: $ultimaPasta")

            } else {

              logFn(
                s"""
                   |✅ OK:
                   |Diretório base: $dir
                   |Última pasta: $ultimaPasta
                   |Quantidade de arquivos: ${arquivos.length}
                   |""".stripMargin
              )
            }

          case None =>

            logFn(
              s"""
                 |⚠️ ALERTA:
                 |Nenhuma pasta encontrada.
                 |Diretório base: $dir
                 |""".stripMargin
            )
        }

      } catch {

        case e: Exception =>

          logFn(
            s"""
               |❌ ERRO ao processar diretório
               |Diretório: $dir
               |Erro: ${e.getMessage}
               |""".stripMargin
          )
      }
    }

    logFn("Finalizada verificação das últimas pastas HDFS")
  }
}

// Execução standalone
// UltimaPastaHDFS.main(Array())