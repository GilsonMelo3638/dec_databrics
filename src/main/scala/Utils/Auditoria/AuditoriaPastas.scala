package Utils.Auditoria

import scala.sys.process._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AuditoriaPastas {
  def main(args: Array[String]): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val ontem = LocalDate.now().minusDays(1)
    val ultimosDezDias = (0 until 10).map(ontem.minusDays(_).format(formatter)).toSet

    val diretorios = Seq(
      "/datalake/bronze/sources/dbms/dec/processamento/nfce/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfe/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/bpe/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/mdfe/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nf3e/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/cte/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfce_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/bpe_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nf3e_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/nfe_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/mdfe_cancelamento/processado",
      "/datalake/bronze/sources/dbms/dec/processamento/cte_cancelamento/processado"
    )

    diretorios.foreach { dir =>
      val comando = s"hdfs dfs -ls $dir"
      val resultado = try {
        comando.!!
      } catch {
        case e: Exception =>
          println(s"Erro ao acessar $dir: ${e.getMessage}")
          ""
      }

      val pastas = resultado
        .split("\n")
        .map(_.split("\\s+").lastOption.getOrElse(""))
        .filter(_.startsWith(dir))
        .map(_.stripPrefix(dir + "/"))
        .filter(_.matches("\\d{8}")) // só pastas com nome no formato yyyyMMdd
        .toSet

      val faltantes = ultimosDezDias.diff(pastas).toList.sorted

      if (faltantes.isEmpty) {
        println(s"✅ Nenhuma lacuna em $dir nos últimos 10 dias até ontem.")
      } else {
        println(s"⚠️  Faltando em $dir: ${faltantes.mkString(", ")}")
      }
    }
  }
}
//AuditoriaPastas.main(Array())