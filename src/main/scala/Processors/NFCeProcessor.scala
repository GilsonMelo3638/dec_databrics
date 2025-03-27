package Processors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NFCeProcessor {
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
      $"DHEMI",
      $"IP_TRANSMISSOR",
      $"parsed.protNFe.infProt._Id".as("infprot_Id"),
      $"parsed.protNFe.infProt.chNFe".as("chave"),
      $"parsed.protNFe.infProt.cStat".as("infprot_cstat"),
      $"parsed.protNFe.infProt.dhRecbto".as("infprot_dhrecbto"),
      $"parsed.protNFe.infProt.digVal".as("infprot_digVal"),
      $"parsed.protNFe.infProt.nProt".as("infprot_nProt"),
      $"parsed.protNFe.infProt.tpAmb".as("infprot_tpAmb"),
      $"parsed.protNFe.infProt.verAplic".as("infprot_verAplic"),
      $"parsed.protNFe.infProt.xMotivo".as("infprot_xMotivo"),
      $"parsed.NFe.infNFe.avulsa.CNPJ".as("avulsa_cnpj"),
      $"parsed.NFe.infNFe.avulsa.UF".as("avulsa_uf"),
      $"parsed.NFe.infNFe.avulsa.dEmi".as("avulsa_demi"),
      $"parsed.NFe.infNFe.avulsa.dPag".as("avulsa_dpag"),
      $"parsed.NFe.infNFe.avulsa.fone".as("avulsa_fone"),
      $"parsed.NFe.infNFe.avulsa.matr".as("avulsa_matr"),
      $"parsed.NFe.infNFe.avulsa.nDAR".as("avulsa_ndar"),
      $"parsed.NFe.infNFe.avulsa.repEmi".as("avulsa_repemi"),
      $"parsed.NFe.infNFe.avulsa.vDAR".as("avulsa_vdar"),
      $"parsed.NFe.infNFe.avulsa.xAgente".as("avulsa_xagente"),
      $"parsed.NFe.infNFe.avulsa.xOrgao".as("avulsa_xorgao"),
      $"parsed.NFe.infNFe.cobr.dup".as("cobr_dup"),
      $"parsed.NFe.infNFe.cobr.fat.nFat".as("cobr_fat_nfat"),
      $"parsed.NFe.infNFe.cobr.fat.vDesc".as("cobr_fat_vdesc"),
      $"parsed.NFe.infNFe.cobr.fat.vLiq".as("cobr_fat_vliq"),
      $"parsed.NFe.infNFe.cobr.fat.vOrig".as("cobr_fat_vorig"),
      $"parsed.NFe.infNFe.compra.xCont".as("compra_xcont"),
      $"parsed.NFe.infNFe.compra.xNEmp".as("compra_xnemp"),
      $"parsed.NFe.infNFe.compra.xPed".as("compra_xped"),
      $"parsed.NFe.infNFe.dest.CNPJ".as("dest_cnpj"),
      $"parsed.NFe.infNFe.dest.CPF".as("dest_cpf"),
      $"parsed.NFe.infNFe.dest.IE".as("dest_ie"),
      $"parsed.NFe.infNFe.dest.IM".as("dest_im"),
      $"parsed.NFe.infNFe.dest.ISUF".as("dest_isuf"),
      $"parsed.NFe.infNFe.dest.email".as("dest_email"),
      $"parsed.NFe.infNFe.dest.enderDest.CEP".as("enderdest_cep"),
      $"parsed.NFe.infNFe.dest.enderDest.UF".as("enderdest_uf"),
      $"parsed.NFe.infNFe.dest.enderDest.cMun".as("enderdest_cmun"),
      $"parsed.NFe.infNFe.dest.enderDest.cPais".as("enderdest_cpais"),
      $"parsed.NFe.infNFe.dest.enderDest.fone".as("enderdest_fone"),
      $"parsed.NFe.infNFe.dest.enderDest.nro".as("enderdest_nro"),
      $"parsed.NFe.infNFe.dest.enderDest.xBairro".as("enderdest_xbairro"),
      $"parsed.NFe.infNFe.dest.enderDest.xCpl".as("enderdest_xcpl"),
      $"parsed.NFe.infNFe.dest.enderDest.xLgr".as("enderdest_xlgr"),
      $"parsed.NFe.infNFe.dest.enderDest.xMun".as("enderdest_xmun"),
      $"parsed.NFe.infNFe.dest.enderDest.xPais".as("enderdest_xpais"),
      $"parsed.NFe.infNFe.dest.xNome".as("dest_xnome"),
      $"parsed.NFe.infNFe.dest.idEstrangeiro".as("idEstrangeiro"),
      $"parsed.NFe.infNFe.dest.indIEDest".as("indIEDest"),
      $"parsed.NFe.infNFe.emit.CNPJ".as("cnpj_emitente"),
      $"parsed.NFe.infNFe.emit.CPF".as("cpf_emitente"),
      $"parsed.NFe.infNFe.emit.CNPJ".as("emit_cnpj"),
      $"parsed.NFe.infNFe.emit.CPF".as("emit_cpf"),
      $"parsed.NFe.infNFe.emit.CNAE".as("emit_cnae"),
      $"parsed.NFe.infNFe.emit.CRT".as("emit_crt"),
      $"parsed.NFe.infNFe.emit.IE".as("emit_ie"),
      $"parsed.NFe.infNFe.emit.IEST".as("emit_iest"),
      $"parsed.NFe.infNFe.emit.IM".as("emit_im"),
      $"parsed.NFe.infNFe.emit.enderEmit.CEP".as("enderemit_cep"),
      $"parsed.NFe.infNFe.emit.enderEmit.UF".as("enderemit_uf"),
      $"parsed.NFe.infNFe.emit.enderEmit.cMun".as("enderemit_cmun"),
      $"parsed.NFe.infNFe.emit.enderEmit.cPais".as("enderemit_cpais"),
      $"parsed.NFe.infNFe.emit.enderEmit.fone".as("enderemit_fone"),
      $"parsed.NFe.infNFe.emit.enderEmit.nro".as("enderemit_nro"),
      $"parsed.NFe.infNFe.emit.enderEmit.xBairro".as("enderemit_xbairro"),
      $"parsed.NFe.infNFe.emit.enderEmit.xCpl".as("enderemit_xcpl"),
      $"parsed.NFe.infNFe.emit.enderEmit.xLgr".as("enderemit_xlgr"),
      $"parsed.NFe.infNFe.emit.enderEmit.xMun".as("enderemit_xmun"),
      $"parsed.NFe.infNFe.emit.enderEmit.xPais".as("enderemit_xpais"),
      $"parsed.NFe.infNFe.emit.xFant".as("emit_xfant"),
      $"parsed.NFe.infNFe.emit.xNome".as("emit_xnome"),
      $"parsed.NFe.infNFe.entrega.CEP".as("entrega_cep"),
      $"parsed.NFe.infNFe.entrega.CNPJ".as("entrega_cnpj"),
      $"parsed.NFe.infNFe.entrega.IE".as("entrega_ie"),
      $"parsed.NFe.infNFe.entrega.CPF".as("entrega_cpf"),
      $"parsed.NFe.infNFe.entrega.UF".as("entrega_uf"),
      $"parsed.NFe.infNFe.entrega.cMun".as("entrega_cmun"),
      $"parsed.NFe.infNFe.entrega.cPais".as("entrega_cpais"),
      $"parsed.NFe.infNFe.entrega.email".as("entrega_email"),
      $"parsed.NFe.infNFe.entrega.fone".as("entrega_fone"),
      $"parsed.NFe.infNFe.entrega.nro".as("entrega_nro"),
      $"parsed.NFe.infNFe.entrega.xBairro".as("entrega_xbairro"),
      $"parsed.NFe.infNFe.entrega.xCpl".as("entrega_xcpl"),
      $"parsed.NFe.infNFe.entrega.xLgr".as("entrega_xlgr"),
      $"parsed.NFe.infNFe.entrega.xMun".as("entrega_xmun"),
      $"parsed.NFe.infNFe.entrega.xNome".as("entrega_xnome"),
      $"parsed.NFe.infNFe.entrega.xPais".as("entrega_xpais"),
      $"parsed.NFe.infNFe.exporta.UFSaidaPais".as("exporta_ufsaidapais"),
      $"parsed.NFe.infNFe.exporta.xLocDespacho".as("exporta_xlocdespacho"),
      $"parsed.NFe.infNFe.exporta.xLocExporta".as("exporta_xlocexporta"),
      $"parsed.NFe.infNFe.ide.dhEmi".as("ide_dhemi"),
      $"parsed.NFe.infNFe.ide.cDV".as("ide_cdv"),
      $"parsed.NFe.infNFe.ide.cMunFG".as("ide_cmunfg"),
      $"parsed.NFe.infNFe.ide.cNF".as("ide_cnf"),
      $"parsed.NFe.infNFe.ide.cUF".as("ide_cuf"),
      $"parsed.NFe.infNFe.ide.dhCont".as("ide_dhcont"),
      $"parsed.NFe.infNFe.ide.dhSaiEnt".as("ide_dhsaient"),
      $"parsed.NFe.infNFe.ide.finNFe".as("ide_finnfe"),
      $"parsed.NFe.infNFe.ide.idDest".as("ide_iddest"),
      $"parsed.NFe.infNFe.ide.indFinal".as("ide_indfinal"),
      $"parsed.NFe.infNFe.ide.indIntermed".as("ide_indintermed"),
      $"parsed.NFe.infNFe.ide.indPres".as("ide_indpres"),
      $"parsed.NFe.infNFe.ide.mod".as("ide_mod"),
      $"parsed.NFe.infNFe.ide.nNF".as("ide_nnfe"),
      $"parsed.NFe.infNFe.ide.natOp".as("ide_natop"),
      $"parsed.NFe.infNFe.ide.procEmi".as("ide_procemi"),
      $"parsed.NFe.infNFe.ide.serie".as("ide_serie"),
      $"parsed.NFe.infNFe.ide.tpAmb".as("ide_tpamb"),
      $"parsed.NFe.infNFe.ide.tpEmis".as("ide_tpemis"),
      $"parsed.NFe.infNFe.ide.tpImp".as("ide_tpimp"),
      $"parsed.NFe.infNFe.ide.tpNF".as("ide_tpnf"),
      $"parsed.NFe.infNFe.ide.verProc".as("ide_verproc"),
      $"parsed.NFe.infNFe.ide.xJust".as("ide_xjust"),
      $"parsed.NFe.infNFe.ide.NFref".as("ide_nfref"),
      $"parsed.NFe.infNFe.retirada.CEP".as("retirada_cep"),
      $"parsed.NFe.infNFe.retirada.CNPJ".as("retirada_cnpj"),
      $"parsed.NFe.infNFe.retirada.CPF".as("retirada_cpf"),
      $"parsed.NFe.infNFe.retirada.IE".as("retirada_ie"),
      $"parsed.NFe.infNFe.retirada.UF".as("retirada_uf"),
      $"parsed.NFe.infNFe.retirada.cMun".as("retirada_cmun"),
      $"parsed.NFe.infNFe.retirada.cPais".as("retirada_cpais"),
      $"parsed.NFe.infNFe.retirada.email".as("retirada_email"),
      $"parsed.NFe.infNFe.retirada.fone".as("retirada_fone"),
      $"parsed.NFe.infNFe.retirada.nro".as("retirada_nro"),
      $"parsed.NFe.infNFe.retirada.xBairro".as("retirada_xbairro"),
      $"parsed.NFe.infNFe.retirada.xCpl".as("retirada_xcpl"),
      $"parsed.NFe.infNFe.retirada.xLgr".as("retirada_xlgr"),
      $"parsed.NFe.infNFe.retirada.xMun".as("retirada_xmun"),
      $"parsed.NFe.infNFe.retirada.xNome".as("retirada_xnome"),
      $"parsed.NFe.infNFe.retirada.xPais".as("retirada_xpais"),
      $"parsed.NFe.infNFe.retirada.modFrete".as("retirada_modfrete"),
      $"parsed.NFe.infNFe.retTransp.cfop".as("rettransp_cfop"),
      $"parsed.NFe.infNFe.retTransp.cmunfg".as("rettransp_cmunfg"),
      $"parsed.NFe.infNFe.retTransp.picmsret".as("rettransp_picmsret"),
      $"parsed.NFe.infNFe.retTransp.vbcret".as("rettransp_vbcret"),
      $"parsed.NFe.infNFe.retTransp.vicmsret".as("rettransp_vicmsret"),
      $"parsed.NFe.infNFe.retTransp.vserv".as("rettransp_vserv"),
      $"parsed.NFe.infNFe.total.icmstot.qbcmono".as("icmstot_qbcmono"),
      $"parsed.NFe.infNFe.total.icmstot.qbcmonoret".as("icmstot_qbcmonoret"),
      $"parsed.NFe.infNFe.total.icmstot.qbcmonoreten".as("icmstot_qbcmonoreten"),
      $"parsed.NFe.infNFe.total.icmstot.vbc".as("icmstot_vbc"),
      $"parsed.NFe.infNFe.total.icmstot.vbcst".as("icmstot_vbcst"),
      $"parsed.NFe.infNFe.total.icmstot.vcofins".as("icmstot_vcofins"),
      $"parsed.NFe.infNFe.total.icmstot.vdesc".as("icmstot_vdesc"),
      $"parsed.NFe.infNFe.total.icmstot.vfcp".as("icmstot_vfcp"),
      $"parsed.NFe.infNFe.total.icmstot.vfcpst".as("icmstot_vfcpst"),
      $"parsed.NFe.infNFe.total.icmstot.vfcpstret".as("icmstot_vfcpstret"),
      $"parsed.NFe.infNFe.total.icmstot.vfcpufdest".as("icmstot_vfcpufdest"),
      $"parsed.NFe.infNFe.total.icmstot.vfrete".as("icmstot_vfrete"),
      $"parsed.NFe.infNFe.total.icmstot.vicms".as("icmstot_vicms"),
      $"parsed.NFe.infNFe.total.icmstot.vicmsdeson".as("icmstot_vicmsdeson"),
      $"parsed.NFe.infNFe.total.icmstot.vicmsmono".as("icmstot_vicmsmono"),
      $"parsed.NFe.infNFe.total.icmstot.vicmsmonoret".as("icmstot_vicmsmonoret"),
      $"parsed.NFe.infNFe.total.icmstot.vicmsmonoreten".as("icmstot_vicmsmonoreten"),
      $"parsed.NFe.infNFe.total.icmstot.vicmsufdest".as("icmstot_vicmsufdest"),
      $"parsed.NFe.infNFe.total.icmstot.vicmsufremet".as("icmstot_vicmsufremet"),
      $"parsed.NFe.infNFe.total.icmstot.vii".as("icmstot_vii"),
      $"parsed.NFe.infNFe.total.icmstot.vipi".as("icmstot_vipi"),
      $"parsed.NFe.infNFe.total.icmstot.vipidevol".as("icmstot_vipidevol"),
      $"parsed.NFe.infNFe.total.icmstot.vnf".as("icmstot_vnf"),
      $"parsed.NFe.infNFe.total.icmstot.voutro".as("icmstot_voutro"),
      $"parsed.NFe.infNFe.total.icmstot.vpis".as("icmstot_vpis"),
      $"parsed.NFe.infNFe.total.icmstot.vprod".as("icmstot_vprod"),
      $"parsed.NFe.infNFe.total.icmstot.vst".as("icmstot_vst"),
      $"parsed.NFe.infNFe.total.icmstot.vseg".as("icmstot_vseg"),
      $"parsed.NFe.infNFe.total.icmstot.vtottrib".as("icmstot_vtottrib"),
      $"parsed.NFe.infNFe.total.ISSQNtot.cRegTrib".as("issqntot_cregtrib"),
      $"parsed.NFe.infNFe.total.ISSQNtot.dCompet".as("issqntot_dcompet"),
      $"parsed.NFe.infNFe.total.ISSQNtot.vBC".as("issqntot_vbc"),
      $"parsed.NFe.infNFe.total.ISSQNtot.vCOFINS".as("issqntot_vcofins"),
      $"parsed.NFe.infNFe.total.ISSQNtot.vISS".as("issqntot_viss"),
      $"parsed.NFe.infNFe.total.ISSQNtot.vPIS".as("issqntot_vpis"),
      $"parsed.NFe.infNFe.total.ISSQNtot.vServ".as("issqntot_vserv"),
      $"parsed.NFe.infNFe.total.retTrib.vBCIRRF".as("rettrib_vbcirrf"),
      $"parsed.NFe.infNFe.total.retTrib.vBCRetPrev".as("rettrib_vbcretprev"),
      $"parsed.NFe.infNFe.total.retTrib.vIRRF".as("rettrib_virrf"),
      $"parsed.NFe.infNFe.total.retTrib.vRetCOFINS".as("rettrib_vretcofins"),
      $"parsed.NFe.infNFe.total.retTrib.vRetCSLL".as("rettrib_vretcsll"),
      $"parsed.NFe.infNFe.total.retTrib.vRetPIS".as("rettrib_vretpis"),
      $"parsed.NFe.infNFe.pag.detPag".as("pag_detPag"),
      $"parsed.NFe.infNFe.pag.vTroco".as("pag_vtroco"),
      $"parsed.NFe.infNFe.transp.modFrete".as("transp_modFrete"),
      $"parsed.NFe.infNFe.transp.reboque".as("transp_reboque"),
      $"parsed.NFe.infNFe.transp.retTransp.CFOP".as("transp_retTransp_CFOP"),
      $"parsed.NFe.infNFe.transp.retTransp.cMunFG".as("transp_retTransp_cMunFG"),
      $"parsed.NFe.infNFe.transp.retTransp.pICMSRet".as("transp_retTransp_pICMSRet"),
      $"parsed.NFe.infNFe.transp.retTransp.vBCRet".as("transp_retTransp_vBCRet"),
      $"parsed.NFe.infNFe.transp.retTransp.vICMSRet".as("transp_retTransp_vICMSRet"),
      $"parsed.NFe.infNFe.transp.retTransp.vServ".as("transp_retTransp_vServ"),
      $"parsed.NFe.infNFe.transp.transporta.CNPJ".as("transp_transporta_CNPJ"),
      $"parsed.NFe.infNFe.transp.transporta.CPF".as("transp_transporta_CPF"),
      $"parsed.NFe.infNFe.transp.transporta.IE".as("transp_transporta_IE"),
      $"parsed.NFe.infNFe.transp.transporta.UF".as("transp_transporta_UF"),
      $"parsed.NFe.infNFe.transp.transporta.xEnder".as("transp_transporta_xEnder"),
      $"parsed.NFe.infNFe.transp.transporta.xMun".as("transp_transporta_xMunJ"),
      $"parsed.NFe.infNFe.transp.transporta.xNome".as("transp_transporta_xNome"),
      $"parsed.NFe.infNFe.transp.transporta".as("transp_transporta"),
      $"parsed.NFe.infNFe.transp.vagao".as("transp_vagao"),
      $"parsed.NFe.infNFe.transp.veicTransp.RNTC".as("transp_veicTransp_RNTC"),
      $"parsed.NFe.infNFe.transp.veicTransp.UF".as("transp_veicTransp_UF"),
      $"parsed.NFe.infNFe.transp.veicTransp.placa".as("transp_veicTransp_placa"),
      $"parsed.NFe.infNFe.transp.vol".as("transp_vol"),
      $"parsed.NFe.infNFe.infAdic.infAdFisco".as("infadic_infadfisco"),
      $"parsed.NFe.infNFe.infAdic.infCpl".as("infadic_infcpl"),
      $"parsed.NFe.infNFe.infAdic.obsCont".as("infadic_obscont"), // Array de structs
      $"parsed.NFe.infNFe.infAdic.obsFisco".as("infadic_obsfisco"), // Array de structs
      $"parsed.NFe.infNFe.infAdic.procRef".as("infadic_procref"), // Array de structs
      $"parsed.NFe.infNFe.infIntermed.CNPJ".as("infintermed_cnpj"),
      $"parsed.NFe.infNFe.infIntermed.idCadIntTran".as("infintermed_idcadinttran"),
      $"parsed.NFe.infNFe.infRespTec.CNPJ".as("infresptec_cnpj"),
      $"parsed.NFe.infNFe.infRespTec.email".as("infresptec_email"),
      $"parsed.NFe.infNFe.infRespTec.fone".as("infresptec_fone"),
      $"parsed.NFe.infNFe.infRespTec.hashCSRT".as("infresptec_hashcsrt"),
      $"parsed.NFe.infNFe.infRespTec.idCSRT".as("infresptec_idcsrt"),
      $"parsed.NFe.infNFe.infRespTec.xContato".as("infresptec_xcontato"),
      $"parsed.NFe.infNFe.infSolicNFF.xSolic".as("infsolicnff_xsolic"),
      $"parsed.NFe.infNFeSupl.qrCode".as("qrCode"),
      $"parsed.NFe.infNFeSupl.urlChave".as("urlChave")
    )
  }
}