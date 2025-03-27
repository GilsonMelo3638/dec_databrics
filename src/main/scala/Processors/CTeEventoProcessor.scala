package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CTeEventoProcessor {
  def generateSelectedDF(parsedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    parsedDF.select(
      $"NSUSVD",
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
      $"parsed.eventoCTe._versao".as("eventoCTe_versao"),
      $"parsed.eventoCTe.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.eventoCTe.infEvento._Id".as("infEvento_Id"),
      $"parsed.eventoCTe.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.eventoCTe.infEvento.chCTe".as("chave"),
      $"parsed.eventoCTe.infEvento.detEvento._versaoEvento".as("detEvento_versaoEvento"),
      $"parsed.eventoCTe.infEvento.detEvento.evCancCTe.descEvento".as("evCancCTe_descEvento"),
      $"parsed.eventoCTe.infEvento.detEvento.evCancCTe.nProt".as("evCancCTe_nProt"),
      $"parsed.eventoCTe.infEvento.detEvento.evCancCTe.xJust".as("evCancCTe_xJust"),
      $"parsed.eventoCTe.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.eventoCTe.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.eventoCTe.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.eventoCTe.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.retEventoCTe._versao".as("retEvento_versao"),
      $"parsed.retEventoCTe.infEvento._Id".as("retInfEvento_Id"),
      $"parsed.retEventoCTe.infEvento.cOrgao".as("retInfEvento_cOrgao"),
      $"parsed.retEventoCTe.infEvento.cStat".as("retInfEvento_cStat"),
      $"parsed.retEventoCTe.infEvento.chCTe".as("retInfEvento_chCTe"),
      $"parsed.retEventoCTe.infEvento.dhRegEvento".as("retInfEvento_dhRegEvento"),
      $"parsed.retEventoCTe.infEvento.nProt".as("retInfEvento_nProt"),
      $"parsed.retEventoCTe.infEvento.nSeqEvento".as("retInfEvento_nSeqEvento"),
      $"parsed.retEventoCTe.infEvento.tpAmb".as("retInfEvento_tpAmb"),
      $"parsed.retEventoCTe.infEvento.tpEvento".as("retInfEvento_tpEvento"),
      $"parsed.retEventoCTe.infEvento.verAplic".as("retInfEvento_verAplic"),
      $"parsed.retEventoCTe.infEvento.xEvento".as("retInfEvento_xEvento"),
      $"parsed.retEventoCTe.infEvento.xMotivo".as("retInfEvento_xMotivo")
    )
  }
}