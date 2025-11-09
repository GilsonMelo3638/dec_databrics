package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NF3eEventoProcessor {
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
      $"parsed.eventoNF3e._versao".as("eventoNF3e_versao"),
      $"parsed.eventoNF3e.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.eventoNF3e.infEvento._Id".as("infEvento_Id"),
      $"parsed.eventoNF3e.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.eventoNF3e.infEvento.chNF3e".as("infEvento_chNF3e"),
      $"parsed.eventoNF3e.infEvento.detEvento._versaoEvento".as("detEvento_versaoEvento"),
      $"parsed.eventoNF3e.infEvento.detEvento.evCancNF3e.descEvento".as("evCancNF3e_descEvento"),
      $"parsed.eventoNF3e.infEvento.detEvento.evCancNF3e.chNF3eSubstituta".as("chNF3eSubstituta"),
      $"parsed.eventoNF3e.infEvento.detEvento.evCancNF3e.nProt".as("evCancNF3e_nProt"),
      $"parsed.eventoNF3e.infEvento.detEvento.evCancNF3e.xJust".as("evCancNF3e_xJust"),
      $"parsed.eventoNF3e.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.eventoNF3e.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.eventoNF3e.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.eventoNF3e.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.retEventoNF3e._versao".as("retEventoNF3e_versao"),
      $"parsed.retEventoNF3e.infEvento._Id".as("retEventoNF3e_Id"),
      $"parsed.retEventoNF3e.infEvento.cOrgao".as("retEventoNF3e_cOrgao"),
      $"parsed.retEventoNF3e.infEvento.cStat".as("retEventoNF3e_cStat"),
      $"parsed.retEventoNF3e.infEvento.chNF3e".as("chave"),
      $"parsed.retEventoNF3e.infEvento.dhRegEvento".as("retEventoNF3e_dhRegEvento"),
      $"parsed.retEventoNF3e.infEvento.nProt".as("retEventoNF3e_nProt"),
      $"parsed.retEventoNF3e.infEvento.nSeqEvento".as("retEventoNF3e_nSeqEvento"),
      $"parsed.retEventoNF3e.infEvento.tpAmb".as("retEventoNF3e_tpAmb"),
      $"parsed.retEventoNF3e.infEvento.tpEvento".as("retEventoNF3e_tpEvento"),
      $"parsed.retEventoNF3e.infEvento.verAplic".as("retEventoNF3e_verAplic"),
      $"parsed.retEventoNF3e.infEvento.xEvento".as("retEventoNF3e_xEvento"),
      $"parsed.retEventoNF3e.infEvento.xMotivo".as("retEventoNF3e_xMotivo")
    )
  }
}