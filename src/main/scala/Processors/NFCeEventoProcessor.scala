package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NFCeEventoProcessor {
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
      $"parsed.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.infEvento._Id".as("infEvento_Id"),
      $"parsed.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.infEvento.chNFe".as("infEvento_chNFe"),
      $"parsed.infEvento.detEvento._versao".as("detEvento_versao"),
      $"parsed.infEvento.detEvento.cOrgaoAutor".as("detEvento_cOrgaoAutor"),
      $"parsed.infEvento.detEvento.chNFeRef".as("detEvento_chNFeRef"),
      $"parsed.infEvento.detEvento.descEvento".as("detEvento_descEvento"),
      $"parsed.infEvento.detEvento.nProt".as("detEvento_nProt"),
      $"parsed.infEvento.detEvento.tpAutor".as("detEvento_tpAutor"),
      $"parsed.infEvento.detEvento.verAplic".as("detEvento_verAplic"),
      $"parsed.infEvento.detEvento.xJust".as("detEvento_xJust"),
      $"parsed.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.infEvento.verEvento".as("infEvento_verEvento"),
      $"parsed.retEvento._versao".as("retEvento_versao"),
      $"parsed.retEvento.infEvento.CNPJDest".as("retEvento_CNPJDest"),
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
      $"parsed.retEvento.infEvento.xMotivo".as("retEvento_xMotivo")
    )
  }
}