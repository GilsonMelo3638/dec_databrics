package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NFeEventoProcessor {
  def generateSelectedDF(parsedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._ // Habilita o uso de $"coluna"
    parsedDF.select(
      $"NSUDF",
      concat(
        substring($"DHPROC", 7, 4),
        substring($"DHPROC", 4, 2),
        substring($"DHPROC", 1, 2),
        substring($"DHPROC", 12, 2)
      ).as("DHPROC_FORMATADO"),
      $"DHEVENTO",
      $"IP_TRANSMISSOR",
      $"parsed.evento.infEvento._Id".as("infEvento_Id"),
      $"parsed.evento.infEvento.chNFe".as("infEvento_chNFe"),
      $"parsed.evento.infEvento.detEvento._versao".as("detEvento_versao"),
      $"parsed.evento.infEvento.detEvento.descEvento".as("detEvento_descEvento"),
      $"parsed.evento.infEvento.detEvento.nProt".as("detEvento_nProt"),
      $"parsed.evento.infEvento.detEvento.xJust".as("detEvento_xJust"),
      $"parsed.evento.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.evento.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.evento.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.evento.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.evento.infEvento.verEvento".as("infEvento_verEvento"),
      $"parsed.retEvento._versao".as("retEvento_versao"),
      $"parsed.retEvento.infEvento.CNPJDest".as("retEvento_CNPJDest"),
      $"parsed.retEvento.infEvento.CPFDest".as("retEvento_CPFDest"),
      $"parsed.retEvento.infEvento._Id".as("retEvento_Id"),
      $"parsed.retEvento.infEvento.cOrgao".as("retEvento_cOrgao"),
      $"parsed.retEvento.infEvento.cStat".as("retEvento_cStat"),
      $"parsed.retEvento.infEvento.chNFe".as("chave"),
      $"parsed.retEvento.infEvento.dhRegEvento".as("retEvento_dhRegEvento"),
      $"parsed.retEvento.infEvento.emailDest".as("retEvento_emailDest"),
      $"parsed.retEvento.infEvento.nProt".as("retEvento_nProt"),
      $"parsed.retEvento.infEvento.nSeqEvento".as("retEvento_nSeqEvento"),
      $"parsed.retEvento.infEvento.tpAmb".as("retEvento_tpAmb"),
      $"parsed.retEvento.infEvento.tpEvento".as("retEvento_tpEvento"),
      $"parsed.retEvento.infEvento.verAplic".as("retEvento_verAplic"),
      $"parsed.retEvento.infEvento.xEvento".as("retEvento_xEvento"),
      $"parsed.retEvento.infEvento.xMotivo".as("retEvento_xMotivo")
    )
  }
}