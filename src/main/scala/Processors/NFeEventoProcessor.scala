package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NFeEventoProcessor {
  def generateSelectedDF(parsedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._ // Habilita o uso de $"coluna"
    parsedDF.select(

      // ===== CAMPOS ORIGINAIS =====
      $"NSUDF",
      concat(
        substring($"DHPROC", 7, 4),
        substring($"DHPROC", 4, 2),
        substring($"DHPROC", 1, 2),
        substring($"DHPROC", 12, 2)
      ).as("DHPROC_FORMATADO"),
      $"DHEVENTO",
      $"IP_TRANSMISSOR",
      $"parsed._versao".as("parsed_versao"),
      $"parsed._xmlns".as("parsed_xmlns"),

      // ===== evento =====
      $"parsed.evento._versao".as("evento_versao"),

      // ===== infEvento =====
      $"parsed.evento.infEvento.CNPJ".as("infEvento_CNPJ"),
      $"parsed.evento.infEvento.CPF".as("infEvento_CPF"),
      $"parsed.evento.infEvento._Id".as("infEvento_Id"),
      $"parsed.evento.infEvento.cOrgao".as("infEvento_cOrgao"),
      $"parsed.evento.infEvento.chNFe".as("infEvento_chNFe"),
      $"parsed.evento.infEvento.dhEvento".as("infEvento_dhEvento"),
      $"parsed.evento.infEvento.nSeqEvento".as("infEvento_nSeqEvento"),
      $"parsed.evento.infEvento.tpAmb".as("infEvento_tpAmb"),
      $"parsed.evento.infEvento.tpEvento".as("infEvento_tpEvento"),
      $"parsed.evento.infEvento.verEvento".as("infEvento_verEvento"),

      // ===== detEvento =====
      $"parsed.evento.infEvento.detEvento._versao".as("detEvento_versao"),
      $"parsed.evento.infEvento.detEvento.descEvento".as("detEvento_descEvento"),
      $"parsed.evento.infEvento.detEvento.nProt".as("detEvento_nProt"),
      $"parsed.evento.infEvento.detEvento.xJust".as("detEvento_xJust"),

      $"parsed.evento.infEvento.detEvento.CPFOper".as("detEvento_CPFOper"),
      $"parsed.evento.infEvento.detEvento.IE".as("detEvento_IE"),
      $"parsed.evento.infEvento.detEvento.PINe".as("detEvento_PINe"),
      $"parsed.evento.infEvento.detEvento.UFDest".as("detEvento_UFDest"),
      $"parsed.evento.infEvento.detEvento.cOrgaoAutor".as("detEvento_cOrgaoAutor"),
      $"parsed.evento.infEvento.detEvento.cPostoUF".as("detEvento_cPostoUF"),
      $"parsed.evento.infEvento.detEvento.chCTe".as("detEvento_chCTe"),
      $"parsed.evento.infEvento.detEvento.chMDFe".as("detEvento_chMDFe"),
      $"parsed.evento.infEvento.detEvento.chNFeRefte".as("detEvento_chNFeRefte"),
      $"parsed.evento.infEvento.detEvento.dPrevEntrega".as("detEvento_dPrevEntrega"),
      $"parsed.evento.infEvento.detEvento.dVistoria".as("detEvento_dVistoria"),
      $"parsed.evento.infEvento.detEvento.dhEmi".as("detEvento_dhEmi"),
      $"parsed.evento.infEvento.detEvento.dhEntrega".as("detEvento_dhEntrega"),
      $"parsed.evento.infEvento.detEvento.dhHashComprovante".as("detEvento_dhHashComprovante"),
      $"parsed.evento.infEvento.detEvento.dhHashTentativaEntrega".as("detEvento_dhHashTentativaEntrega"),
      $"parsed.evento.infEvento.detEvento.dhPas".as("detEvento_dhPas"),
      $"parsed.evento.infEvento.detEvento.dhTentativaEntrega".as("detEvento_dhTentativaEntrega"),
      $"parsed.evento.infEvento.detEvento.hashComprovante".as("detEvento_hashComprovante"),
      $"parsed.evento.infEvento.detEvento.hashTentativaEntrega".as("detEvento_hashTentativaEntrega"),
      $"parsed.evento.infEvento.detEvento.indOffline".as("detEvento_indOffline"),
      $"parsed.evento.infEvento.detEvento.indQuitacao".as("detEvento_indQuitacao"),
      $"parsed.evento.infEvento.detEvento.indRet".as("detEvento_indRet"),
      $"parsed.evento.infEvento.detEvento.latGPS".as("detEvento_latGPS"),
      $"parsed.evento.infEvento.detEvento.locVistoria".as("detEvento_locVistoria"),
      $"parsed.evento.infEvento.detEvento.longGPS".as("detEvento_longGPS"),
      $"parsed.evento.infEvento.detEvento.nDoc".as("detEvento_nDoc"),
      $"parsed.evento.infEvento.detEvento.nProtCTeCanc".as("detEvento_nProtCTeCanc"),
      $"parsed.evento.infEvento.detEvento.nProtEvento".as("detEvento_nProtEvento"),
      $"parsed.evento.infEvento.detEvento.nTentativa".as("detEvento_nTentativa"),
      $"parsed.evento.infEvento.detEvento.postoVistoria".as("detEvento_postoVistoria"),
      $"parsed.evento.infEvento.detEvento.sentidoVia".as("detEvento_sentidoVia"),
      $"parsed.evento.infEvento.detEvento.tpAutor".as("detEvento_tpAutor"),
      $"parsed.evento.infEvento.detEvento.tpEventoAut".as("detEvento_tpEventoAut"),
      $"parsed.evento.infEvento.detEvento.tpMotivo".as("detEvento_tpMotivo"),
      $"parsed.evento.infEvento.detEvento.tpNF".as("detEvento_tpNF"),
      $"parsed.evento.infEvento.detEvento.verAplic".as("detEvento_verAplic"),
      $"parsed.evento.infEvento.detEvento.xCondUso".as("detEvento_xCondUso"),
      $"parsed.evento.infEvento.detEvento.xCorrecao".as("detEvento_xCorrecao"),
      $"parsed.evento.infEvento.detEvento.xHistorico".as("detEvento_xHistorico"),
      $"parsed.evento.infEvento.detEvento.xJustMotivo".as("detEvento_xJustMotivo"),
      $"parsed.evento.infEvento.detEvento.xNome".as("detEvento_xNome"),
      $"parsed.evento.infEvento.detEvento.xNomeOper".as("detEvento_xNomeOper"),
      $"parsed.evento.infEvento.detEvento.xObs".as("detEvento_xObs"),
      $"parsed.evento.infEvento.detEvento.xPostoUF".as("detEvento_xPostoUF"),

      // ===== STRUCTS MANTIDAS =====
      $"parsed.evento.infEvento.detEvento.CTe".as("detEvento_CTe"),
      $"parsed.evento.infEvento.detEvento.MDFe".as("detEvento_MDFe"),
      $"parsed.evento.infEvento.detEvento.ctg".as("detEvento_ctg"),
      $"parsed.evento.infEvento.detEvento.dest".as("detEvento_dest"),
      $"parsed.evento.infEvento.detEvento.emit".as("detEvento_emit"),
      $"parsed.evento.infEvento.detEvento.modalOutro".as("detEvento_modalOutro"),
      $"parsed.evento.infEvento.detEvento.modalRodov".as("detEvento_modalRodov"),

      // ===== ARRAYS INTACTOS =====
      $"parsed.evento.infEvento.detEvento.detPag".as("detEvento_detPag"),
      $"parsed.evento.infEvento.detEvento.gImobilizacao".as("detEvento_gImobilizacao"),
      $"parsed.evento.infEvento.detEvento.itensAverbados".as("detEvento_itensAverbados"),

      // ===== retEvento =====
      $"parsed.retEvento._versao".as("retEvento_versao"),
      $"parsed.retEvento.infEvento.CNPJDest".as("retEvento_CNPJDest"),
      $"parsed.retEvento.infEvento.CPFDest".as("retEvento_CPFDest"),
      $"parsed.retEvento.infEvento._Id".as("retEvento_Id"),
      $"parsed.retEvento.infEvento.cOrgao".as("retEvento_cOrgao"),
      $"parsed.retEvento.infEvento.cOrgaoAutor".as("retEvento_cOrgaoAutor"),
      $"parsed.retEvento.infEvento.cStat".as("retEvento_cStat"),
      $"parsed.retEvento.infEvento.chCTe".as("retEvento_chCTe"),
      $"parsed.retEvento.infEvento.chMDFe".as("retEvento_chMDFe"),
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