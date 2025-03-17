package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BPeEventoProcessor {
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
      $"parsed.eventoBPe._versao".as("eventoBPe_versao"),
      $"parsed.eventoBPe.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.eventoBPe.infEvento._Id".as("infEvento_Id"),
      $"parsed.eventoBPe.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.eventoBPe.infEvento.chBPe".as("infEvento_chBPe"),
      $"parsed.eventoBPe.infEvento.detEvento._versaoEvento".as("detEvento_versaoEvento"),
      $"parsed.eventoBPe.infEvento.detEvento.evCancBPe.descEvento".as("evCancBPe_descEvento"),
      $"parsed.eventoBPe.infEvento.detEvento.evCancBPe.nProt".as("evCancBPe_nProt"),
      $"parsed.eventoBPe.infEvento.detEvento.evCancBPe.xJust".as("evCancBPe_xJust"),
      $"parsed.eventoBPe.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.eventoBPe.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.eventoBPe.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.eventoBPe.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.retEventoBPe._versao".as("retEventoBPe_versao"),
      $"parsed.retEventoBPe.infEvento._Id".as("retEventoBPe_Id"),
      $"parsed.retEventoBPe.infEvento.cOrgao".as("retEventoBPe_cOrgao"),
      $"parsed.retEventoBPe.infEvento.cStat".as("retEventoBPe_cStat"),
      $"parsed.retEventoBPe.infEvento.chBPe".as("chave"),
      $"parsed.retEventoBPe.infEvento.dhRegEvento".as("retEventoBPe_dhRegEvento"),
      $"parsed.retEventoBPe.infEvento.nProt".as("retEventoBPe_nProt"),
      $"parsed.retEventoBPe.infEvento.nSeqEvento".as("retEventoBPe_nSeqEvento"),
      $"parsed.retEventoBPe.infEvento.tpAmb".as("retEventoBPe_tpAmb"),
      $"parsed.retEventoBPe.infEvento.tpEvento".as("retEventoBPe_tpEvento"),
      $"parsed.retEventoBPe.infEvento.verAplic".as("retEventoBPe_verAplic"),
      $"parsed.retEventoBPe.infEvento.xEvento".as("retEventoBPe_xEvento"),
      $"parsed.retEventoBPe.infEvento.xMotivo".as("retEventoBPe_xMotivo")
    )
  }
}