package Auditoria

import scala.sys.process._

object UltimaPastaHDFS {

  def main(args: Array[String]): Unit = {

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
      "/datalake/bronze/sources/dbms/dec/processamento/mdfe_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nf3e_evento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfcom_evento/processado"
    )

    diretorios.foreach { dir =>
      try {
        val comandoListarPastas = s"hdfs dfs -ls $dir"
        val resultado = comandoListarPastas.!!

        val pastas = resultado.split("\n")
          .filter(_.startsWith("d")) // só diretórios
          .map(_.trim.split("\\s+").last)
          .sorted
          .reverse

        pastas.headOption match {
          case Some(ultimaPasta) =>
            println(s"Última pasta em $dir: $ultimaPasta")

            // Agora verifica se tem arquivos dentro
            val comandoListarArquivos = s"hdfs dfs -ls $ultimaPasta"
            val conteudo = comandoListarArquivos.!!

            val arquivos = conteudo.split("\n")
              .filter(_.startsWith("-")) // arquivos regulares

            if (arquivos.isEmpty) {
              println(
                s"""🚨 ALERTA:
                   |A última pasta está VAZIA!
                   |Diretório base: $dir
                   |Última pasta: $ultimaPasta
                   |Possível falha de processamento ou ingestão.
                   |""".stripMargin
              )

              // Se quiser falhar o job, descomenta:
              // sys.error(s"Última pasta vazia: $ultimaPasta")
            }

          case None =>
            println(s"⚠️ ALERTA: Nenhuma pasta encontrada em $dir")
        }

      } catch {
        case e: Exception =>
          println(s"❌ ERRO ao processar diretório $dir: ${e.getMessage}")
      }
    }
  }
}

// UltimaPastaHDFS.main(Array())