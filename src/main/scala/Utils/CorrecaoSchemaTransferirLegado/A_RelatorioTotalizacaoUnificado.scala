package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.SparkSession

import java.util.Properties

object A_RelatorioTotalizacaoUnificado {

  val ANO = "2026"
  val MES = "02"
  val DATA_INICIO = s"01/$MES/$ANO"
  val DATA_FIM = s"28/$MES/$ANO"

  val BASE_PATH = "/datalake/bronze/sources/dbms/dec/diario"

  case class Fonte(
                    tabelaOracle: String,
                    tipoParquet: String,
                    descricao: String
                  )

  val fontes = Seq(
    Fonte("ADMDEC.DEC_DFE_NFE", "nfe", "NF-e"),
    Fonte("ADMDEC.DEC_DFE_NFE_CANCELAMENTO", "nfe_cancelamento", "Cancelamento NF-e"),
    Fonte("ADMDEC.DEC_DFE_NFE_EVENTO", "nfe_evento", "Evento NF-e"),
    Fonte("ADMDEC.DEC_DFE_NFCE", "nfce", "NFC-e"),
    Fonte("ADMDEC.DEC_DFE_NFCE_CANCELAMENTO", "nfce_cancelamento", "Cancelamento NFC-e"),
    Fonte("ADMDEC.DEC_DFE_NF3E", "nf3e", "NF3-e"),
    Fonte("ADMDEC.DEC_DFE_NF3E_CANCELAMENTO", "nf3e_cancelamento", "Cancelamento NF3-e"),
    Fonte("ADMDEC.DEC_DFE_NF3E_EVENTO", "nf3e_evento", "Evento NF3-e"),
    Fonte("ADMDEC.DEC_DFE_NFCOM", "nfcom", "NFCom"),
    Fonte("ADMDEC.DEC_DFE_NFCOM_CANCELAMENTO", "nfcom_cancelamento", "Cancelamento NFCom"),
    Fonte("ADMDEC.DEC_DFE_NFCOM_EVENTO", "nfcom_evento", "Evento NFCom"),
    Fonte("ADMDEC.DEC_DFE_BPE", "bpe", "BPE"),
    Fonte("ADMDEC.DEC_DFE_BPE_CANCELAMENTO", "bpe_cancelamento", "Cancelamento BPE"),
    Fonte("ADMDEC.DEC_DFE_BPE_EVENTO", "bpe_evento", "Evento BPE"),
    Fonte("ADMDEC.DEC_DFE_CTE_SVD", "cte", "CT-e"),
    Fonte("ADMDEC.DEC_DFE_CTE_SVD_CANCELAMENTO", "cte_cancelamento", "Cancelamento CT-e"),
    Fonte("ADMDEC.DEC_DFE_CTE_SVD_EVENTO", "cte_evento", "Evento CT-e"),
    Fonte("ADMDEC.DEC_DFE_MDFE", "mdfe", "MDF-e"),
    Fonte("ADMDEC.DEC_DFE_MDFE_CANCELAMENTO", "mdfe_cancelamento", "Cancelamento MDF-e"),
    Fonte("ADMDEC.DEC_DFE_MDFE_EVENTO", "mdfe_evento", "Evento MDF-e")
  )

  def extrairTotal(df: org.apache.spark.sql.DataFrame): Long = {
    if (df.isEmpty) return 0L
    df.first().getDecimal(0).longValue()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RelatorioTotalizacaoUnificado")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    import spark.implicits._

    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"

    val props = new Properties()
    props.put("user", "admhadoop")
    props.put("password", ".admhadoop#")
    props.put("driver", "oracle.jdbc.driver.OracleDriver")

    val dataInicial = s"$DATA_INICIO 00:00:00"
    val dataFinal   = s"$DATA_FIM 23:59:59"

    var resultados = Seq.empty[(String, String, Long, Long)]
      .toDF("tabela", "descricao", "quantidade", "quantidade_processado")

    fontes.foreach { f =>

      val totalOracle =
        try {
          val query =
            s"""
               SELECT COUNT(chave) AS TOTAL
               FROM ${f.tabelaOracle}
               WHERE DHPROC BETWEEN
               TO_DATE('$dataInicial','DD/MM/YYYY HH24:MI:SS')
               AND TO_DATE('$dataFinal','DD/MM/YYYY HH24:MI:SS')
             """

          val df = spark.read.jdbc(jdbcUrl, s"($query) tmp", props)

          extrairTotal(df)

        } catch {
          case e: Exception =>
            println(s"ERRO ORACLE ${f.tabelaOracle}: ${e.getMessage}")
            -1L
        }

      val path = s"$BASE_PATH/${f.tipoParquet}/year=$ANO/month=$MES"

      val totalProcessado =
        try spark.read.parquet(path).count()
        catch { case _: Exception => 0L }

      val tabela = f.tabelaOracle.replace("ADMDEC.DEC_DFE_", "")

      val linha = Seq(
        (tabela, f.descricao, totalOracle, totalProcessado)
      ).toDF("tabela", "descricao", "quantidade", "quantidade_processado")

      resultados = resultados.union(linha)
    }

    val finalDF = resultados.orderBy($"tabela")

    finalDF.show(false)

    val divergencias = finalDF
      .withColumn("diferenca", $"quantidade" - $"quantidade_processado")
      .filter($"diferenca" =!= 0)

    println("\nDIVERGENCIAS:")
    divergencias.show(false)

    spark.stop()
  }
}

//RelatorioTotalizacaoUnificado.main(Array())
