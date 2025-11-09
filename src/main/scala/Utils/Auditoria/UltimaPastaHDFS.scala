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
    val comando = s"hdfs dfs -ls $dir"
      val resultado = comando.!!

      val pastas = resultado.split("\n")
        .map(_.split(" ").lastOption.getOrElse(""))
        .filter(_.startsWith(dir))
        .map(_.stripPrefix(dir + "/"))
        .filter(_.nonEmpty)
        .sorted.reverse

      val ultimaPasta = pastas.headOption.getOrElse("Nenhuma pasta encontrada")
      println(s"Última pasta em $dir: $ultimaPasta")
    }
  }
}
//UltimaPastaHDFS.main(Array())

