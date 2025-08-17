package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NFComEventoProcessor {
  def generateSelectedDF(parsedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
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
      $"parsed.eventoNFCom._versao".as("eventoNFCom_versao"),
      $"parsed.eventoNFCom.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.eventoNFCom.infEvento._Id".as("infEvento_Id"),
      $"parsed.eventoNFCom.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.eventoNFCom.infEvento.chNFCom".as("infEvento_chNFCom"),
      $"parsed.eventoNFCom.infEvento.detEvento._versaoEvento".as("detEvento_versaoEvento"),
      $"parsed.eventoNFCom.infEvento.detEvento.evCancNFCom.descEvento".as("evCancNFCom_descEvento"),
      $"parsed.eventoNFCom.infEvento.detEvento.evCancNFCom.nProt".as("evCancNFCom_nProt"),
      $"parsed.eventoNFCom.infEvento.detEvento.evCancNFCom.xJust".as("evCancNFCom_xJust"),
      $"parsed.eventoNFCom.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.eventoNFCom.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.eventoNFCom.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.eventoNFCom.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.retEventoNFCom._versao".as("retEventoNFCom_versao"),
      $"parsed.retEventoNFCom.infEvento._Id".as("retEventoNFCom_Id"),
      $"parsed.retEventoNFCom.infEvento.cOrgao".as("retEventoNFCom_cOrgao"),
      $"parsed.retEventoNFCom.infEvento.cStat".as("retEventoNFCom_cStat"),
      $"parsed.retEventoNFCom.infEvento.chNFCom".as("chave"),
      $"parsed.retEventoNFCom.infEvento.dhRegEvento".as("retEventoNFCom_dhRegEvento"),
      $"parsed.retEventoNFCom.infEvento.nProt".as("retEventoNFCom_nProt"),
      $"parsed.retEventoNFCom.infEvento.nSeqEvento".as("retEventoNFCom_nSeqEvento"),
      $"parsed.retEventoNFCom.infEvento.tpAmb".as("retEventoNFCom_tpAmb"),
      $"parsed.retEventoNFCom.infEvento.tpEvento".as("retEventoNFCom_tpEvento"),
      $"parsed.retEventoNFCom.infEvento.verAplic".as("retEventoNFCom_verAplic"),
      $"parsed.retEventoNFCom.infEvento.xEvento".as("retEventoNFCom_xEvento"),
      $"parsed.retEventoNFCom.infEvento.xMotivo".as("retEventoNFCom_xMotivo")
    )
  }
}