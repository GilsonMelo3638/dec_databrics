package Processors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MDFeEventoProcessor {
  def generateSelectedDF(parsedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._ // Habilita o uso de $"coluna"
    parsedDF.select(
      $"NSU",
      concat(
        substring($"DHPROC", 7, 4),
        substring($"DHPROC", 4, 2),
        substring($"DHPROC", 1, 2),
        substring($"DHPROC", 12, 2)
      ).as("DHPROC_FORMATADO"),
      $"DHEVENTO",
      $"IP_TRANSMISSOR",
      $"parsed._dhConexao".as("dhConexao"),
      $"parsed._ipTransmissor".as("ipTransmissor"),
      $"parsed._nPortaCon".as("nPortaCon"),
      $"parsed._versao".as("versao"),
      $"parsed._xmlns".as("xmlns"),
      $"parsed.eventoMDFe._versao".as("eventoMDFe_versao"),
      $"parsed.eventoMDFe.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.eventoMDFe.infEvento._Id".as("infEvento_Id"),
      $"parsed.eventoMDFe.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.eventoMDFe.infEvento.chMDFe".as("infEvento_chMDFe"),
      $"parsed.eventoMDFe.infEvento.detEvento._versaoEvento".as("detEvento_versaoEvento"),
      $"parsed.eventoMDFe.infEvento.detEvento.evCancMDFe.descEvento".as("evCancMDFe_descEvento"),
      $"parsed.eventoMDFe.infEvento.detEvento.evCancMDFe.nProt".as("evCancMDFe_nProt"),
      $"parsed.eventoMDFe.infEvento.detEvento.evCancMDFe.xJust".as("evCancMDFe_xJust"),
      $"parsed.eventoMDFe.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.eventoMDFe.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.eventoMDFe.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.eventoMDFe.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.retEventoMDFe._versao".as("retEventoMDFe_versao"),
      $"parsed.retEventoMDFe.infEvento._Id".as("retEventoMDFe_Id"),
      $"parsed.retEventoMDFe.infEvento.cOrgao".as("retEventoMDFe_cOrgao"),
      $"parsed.retEventoMDFe.infEvento.cStat".as("retEventoMDFe_cStat"),
      $"parsed.retEventoMDFe.infEvento.chMDFe".as("chave"),
      $"parsed.retEventoMDFe.infEvento.dhRegEvento".as("retEventoMDFe_dhRegEvento"),
      $"parsed.retEventoMDFe.infEvento.nProt".as("retEventoMDFe_nProt"),
      $"parsed.retEventoMDFe.infEvento.nSeqEvento".as("retEventoMDFe_nSeqEvento"),
      $"parsed.retEventoMDFe.infEvento.tpAmb".as("retEventoMDFe_tpAmb"),
      $"parsed.retEventoMDFe.infEvento.tpEvento".as("retEventoMDFe_tpEvento"),
      $"parsed.retEventoMDFe.infEvento.verAplic".as("retEventoMDFe_verAplic"),
      $"parsed.retEventoMDFe.infEvento.xEvento".as("retEventoMDFe_xEvento"),
      $"parsed.retEventoMDFe.infEvento.xMotivo".as("retEventoMDFe_xMotivo")
    )
  }
}