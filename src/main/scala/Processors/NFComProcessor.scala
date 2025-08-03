package Processors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NFComProcessor {
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
      $"DHEMI",
      $"IP_TRANSMISSOR",

      // Campos de infNFCom
      $"parsed.NFCom.infNFCom._Id".as("infnfcom_id"),
      $"parsed.NFCom.infNFCom._versao".as("infnfcom_versao"),

      // Campos do assinante
      $"parsed.NFCom.infNFCom.assinante.NroTermAdic".as("assinante_nrotermadic"),
      $"parsed.NFCom.infNFCom.assinante.NroTermPrinc".as("assinante_nrotermprinc"),
      $"parsed.NFCom.infNFCom.assinante.cUFAdic".as("assinante_cufadic"),
      $"parsed.NFCom.infNFCom.assinante.cUFPrinc".as("assinante_cufprinc"),
      $"parsed.NFCom.infNFCom.assinante.dContratoFim".as("assinante_dcontratofim"),
      $"parsed.NFCom.infNFCom.assinante.dContratoIni".as("assinante_dcontratoini"),
      $"parsed.NFCom.infNFCom.assinante.iCodAssinante".as("assinante_icodassinante"),
      $"parsed.NFCom.infNFCom.assinante.nContrato".as("assinante_ncontrato"),
      $"parsed.NFCom.infNFCom.assinante.tpAssinante".as("assinante_tpassinante"),
      $"parsed.NFCom.infNFCom.assinante.tpServUtil".as("assinante_tpservutil"),

      // Campos de gSub
      $"parsed.NFCom.infNFCom.gSub.chNFCom".as("gsub_chnfcom"),
      $"parsed.NFCom.infNFCom.gSub.gNF.CNPJ".as("gnf_cnpj"),
      $"parsed.NFCom.infNFCom.gSub.gNF.mod".as("gnf_mod"),
      $"parsed.NFCom.infNFCom.gSub.gNF.serie".as("gnf_serie"),
      $"parsed.NFCom.infNFCom.gSub.gNF.nNF".as("gnf_nnf"),
      $"parsed.NFCom.infNFCom.gSub.gNF.CompetEmis".as("gnf_competemis"),
      $"parsed.NFCom.infNFCom.gSub.gNF.hash115".as("gnf_hash115"),
      $"parsed.NFCom.infNFCom.gSub.gNF.motSub".as("gnf_motsub"),

      // Campos de gCofat
      $"parsed.NFCom.infNFCom.gCofat.chNFComLocal".as("gcofat_chnfcomlocal"),

      // Campos de autXML
      $"parsed.NFCom.infNFCom.autXML.CNPJ".as("autxml_cnpj"),
      $"parsed.NFCom.infNFCom.autXML.CPF".as("autxml_cpf"),

      // Campos de dest
      $"parsed.NFCom.infNFCom.dest.xNome".as("dest_xnome"),
      $"parsed.NFCom.infNFCom.dest.CNPJ".as("dest_cnpj"),
      $"parsed.NFCom.infNFCom.dest.CPF".as("dest_cpf"),
      $"parsed.NFCom.infNFCom.dest.idOutros".as("dest_idoutros"),
      $"parsed.NFCom.infNFCom.dest.indIEDest".as("dest_indiedest"),
      $"parsed.NFCom.infNFCom.dest.IE".as("dest_ie"),
      $"parsed.NFCom.infNFCom.dest.IM".as("dest_im"),
      $"parsed.NFCom.infNFCom.dest.enderDest.CEP".as("dest_ender_cep"),
      $"parsed.NFCom.infNFCom.dest.enderDest.UF".as("enderdest_uf"),
      $"parsed.NFCom.infNFCom.dest.enderDest.cMun".as("enderdest_cmun"),
      $"parsed.NFCom.infNFCom.dest.enderDest.cPais".as("enderdest_cpais"),
      $"parsed.NFCom.infNFCom.dest.enderDest.email".as("enderdest_email"),
      $"parsed.NFCom.infNFCom.dest.enderDest.fone".as("enderdest_fone"),
      $"parsed.NFCom.infNFCom.dest.enderDest.nro".as("enderdest_nro"),
      $"parsed.NFCom.infNFCom.dest.enderDest.xBairro".as("enderdest_xbairro"),
      $"parsed.NFCom.infNFCom.dest.enderDest.xCpl".as("enderdest_xcpl"),
      $"parsed.NFCom.infNFCom.dest.enderDest.xLgr".as("enderdest_xlgr"),
      $"parsed.NFCom.infNFCom.dest.enderDest.xMun".as("enderdest_xmun"),
      $"parsed.NFCom.infNFCom.dest.enderDest.xPais".as("enderdest_xpais"),

      // Array det
      $"parsed.NFCom.infNFCom.det".as("det"),

      // Campos de emit
      $"parsed.NFCom.infNFCom.emit.CNPJ".as("emit_cnpj"),
      $"parsed.NFCom.infNFCom.emit.IE".as("emit_ie"),
      $"parsed.NFCom.infNFCom.emit.IEUFDest".as("emit_ieufdest"),
      $"parsed.NFCom.infNFCom.emit.CRT".as("emit_crt"),
      $"parsed.NFCom.infNFCom.emit.xNome".as("emit_xnome"),
      $"parsed.NFCom.infNFCom.emit.xFant".as("emit_xfant"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.CEP".as("enderemit_cep"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.UF".as("enderemit_uf"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.cMun".as("enderemit_cmun"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.email".as("enderemit_email"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.fone".as("enderemit_fone"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.nro".as("enderemit_nro"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.xBairro".as("enderemit_xbairro"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.xCpl".as("enderemit_xcpl"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.xLgr".as("enderemit_xlgr"),
      $"parsed.NFCom.infNFCom.emit.enderEmit.xMun".as("enderemit_xmun"),

      // Campos de gFat
      $"parsed.NFCom.infNFCom.gFat.CompetFat".as("gfat_competfat"),
      $"parsed.NFCom.infNFCom.gFat.codAgencia".as("gfat_codagencia"),
      $"parsed.NFCom.infNFCom.gFat.codBanco".as("gfat_codbanco"),
      $"parsed.NFCom.infNFCom.gFat.codBarras".as("gfat_codbarras"),
      $"parsed.NFCom.infNFCom.gFat.codDebAuto".as("gfat_coddebauto"),
      $"parsed.NFCom.infNFCom.gFat.dPerUsoFim".as("gfat_dperusofim"),
      $"parsed.NFCom.infNFCom.gFat.dPerUsoIni".as("gfat_dperusoini"),
      $"parsed.NFCom.infNFCom.gFat.dVencFat".as("gfat_dvencfat"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.CEP".as("endercorresp_cep"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.UF".as("endercorresp_uf"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.cMun".as("endercorresp_cmun"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.nro".as("endercorresp_nro"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.xBairro".as("endercorresp_xbairro"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.xCpl".as("endercorresp_xcpl"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.xLgr".as("endercorresp_xlgr"),
      $"parsed.NFCom.infNFCom.gFat.enderCorresp.xMun".as("endercorresp_xmun"),
      $"parsed.NFCom.infNFCom.gFat.gPIX.urlQRCodePIX".as("gfat_gpix_urlqrcode"),

      // Campos de gFatCentral
      $"parsed.NFCom.infNFCom.gFatCentral.CNPJ".as("gfatcentral_cnpj"),
      $"parsed.NFCom.infNFCom.gFatCentral.cUF".as("gfatcentral_cuf"),

      // Campos de gRespTec
      $"parsed.NFCom.infNFCom.gRespTec.CNPJ".as("gresptec_cnpj"),
      $"parsed.NFCom.infNFCom.gRespTec.email".as("gresptec_email"),
      $"parsed.NFCom.infNFCom.gRespTec.fone".as("gresptec_fone"),
      $"parsed.NFCom.infNFCom.gRespTec.xContato".as("gresptec_xcontato"),

      // Campos de ide
      $"parsed.NFCom.infNFCom.ide.cDV".as("ide_cdv"),
      $"parsed.NFCom.infNFCom.ide.cMunFG".as("ide_cmunfg"),
      $"parsed.NFCom.infNFCom.ide.cNF".as("ide_cnf"),
      $"parsed.NFCom.infNFCom.ide.cUF".as("ide_cuf"),
      $"parsed.NFCom.infNFCom.ide.dhEmi".as("ide_dhemi"),
      $"parsed.NFCom.infNFCom.ide.dhCont".as("ide_dhcont"),
      $"parsed.NFCom.infNFCom.ide.finNFCom".as("ide_finnfcom"),
      $"parsed.NFCom.infNFCom.ide.indCessaoMeiosRede".as("ide_indcessaomeiosrede"),
      $"parsed.NFCom.infNFCom.ide.indNotaEntrada".as("ide_indnotaentrada"),
      $"parsed.NFCom.infNFCom.ide.indPrePago".as("ide_indprepago"),
      $"parsed.NFCom.infNFCom.ide.mod".as("ide_mod"),
      $"parsed.NFCom.infNFCom.ide.nNF".as("ide_nnf"),
      $"parsed.NFCom.infNFCom.ide.nSiteAutoriz".as("ide_nsiteautoriz"),
      $"parsed.NFCom.infNFCom.ide.serie".as("ide_serie"),
      $"parsed.NFCom.infNFCom.ide.tpAmb".as("ide_tpamb"),
      $"parsed.NFCom.infNFCom.ide.tpEmis".as("ide_tpemis"),
      $"parsed.NFCom.infNFCom.ide.tpFat".as("ide_tpfat"),
      $"parsed.NFCom.infNFCom.ide.verProc".as("ide_verproc"),
      $"parsed.NFCom.infNFCom.ide.xJust".as("ide_xjust"),

      // Campos de infAdic
      $"parsed.NFCom.infNFCom.infAdic.infAdFisco".as("infadic_infadfisco"),
      $"parsed.NFCom.infNFCom.infAdic.infCpl".as("infadic_infcpl"),

      // Campos de total
      $"parsed.NFCom.infNFCom.total.ICMSTot.vBC".as("icmstot_vbc"),
      $"parsed.NFCom.infNFCom.total.ICMSTot.vFCP".as("icmstot_vfcp"),
      $"parsed.NFCom.infNFCom.total.ICMSTot.vICMS".as("icmstot_vicms"),
      $"parsed.NFCom.infNFCom.total.ICMSTot.vICMSDeson".as("icmstot_vicmsdeson"),
      $"parsed.NFCom.infNFCom.total.vCOFINS".as("total_vcofins"),
      $"parsed.NFCom.infNFCom.total.vDesc".as("total_vdesc"),
      $"parsed.NFCom.infNFCom.total.vFUNTTEL".as("total_vfunttel"),
      $"parsed.NFCom.infNFCom.total.vFUST".as("total_vfust"),
      $"parsed.NFCom.infNFCom.total.vNF".as("total_vnf"),
      $"parsed.NFCom.infNFCom.total.vOutro".as("total_voutro"),
      $"parsed.NFCom.infNFCom.total.vPIS".as("total_vpis"),
      $"parsed.NFCom.infNFCom.total.vProd".as("total_vprod"),
      $"parsed.NFCom.infNFCom.total.vRetTribTot.vIRRF".as("vrettribtot_virrf"),
      $"parsed.NFCom.infNFCom.total.vRetTribTot.vRetCSLL".as("vrettribtot_vretcsll"),
      $"parsed.NFCom.infNFCom.total.vRetTribTot.vRetCofins".as("vrettribtot_vretcofins"),
      $"parsed.NFCom.infNFCom.total.vRetTribTot.vRetPIS".as("vrettribtot_vretpis"),

      // Campos de infNFComSupl
      $"parsed.NFCom.infNFComSupl.qrCodNFCom".as("infnfcomsupl_qrcodnfcom"),

      // Campos do root
      $"parsed._dhConexao",
      $"parsed._ipTransmissor",
      $"parsed._nPortaCon",
      $"parsed._versao",
      $"parsed._xmlns",

      // Campos de protNFCom
      $"parsed.protNFCom._versao".as("protnfcom_versao"),
      $"parsed.protNFCom.infProt._Id".as("infprot_id"),
      $"parsed.protNFCom.infProt.cStat".as("infprot_cstat"),
      $"parsed.protNFCom.infProt.chNFCom".as("chave"),
      $"parsed.protNFCom.infProt.dhRecbto".as("infprot_dhrecbto"),
      $"parsed.protNFCom.infProt.digVal".as("infprot_digval"),
      $"parsed.protNFCom.infProt.nProt".as("infprot_nprot"),
      $"parsed.protNFCom.infProt.tpAmb".as("infprot_tpamb"),
      $"parsed.protNFCom.infProt.verAplic".as("infprot_veraplic"),
      $"parsed.protNFCom.infProt.xMotivo".as("infprot_xmotivo")
    )
  }
}