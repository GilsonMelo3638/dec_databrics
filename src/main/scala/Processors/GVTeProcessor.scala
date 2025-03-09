package Processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GVTeProcessor {
  def generateSelectedDF(parsedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._ // Habilita o uso de $"coluna"

    parsedDF.select(
      $"NSUSVD",
      concat(
        substring($"DHPROC", 7, 4),
        substring($"DHPROC", 4, 2),
        substring($"DHPROC", 1, 2),
        substring($"DHPROC", 12, 2)
      ).as("DHPROC_FORMATADO"),
      $"DHEMI",
      $"IP_TRANSMISSOR",
      $"MODELO",
      $"TPEMIS",
      $"parsed.protCTe.infProt._Id".as("infProt_id"),
      $"parsed.protCTe.infProt.cStat".as("infprot_cstat"),
      $"parsed.protCTe.infProt.chCTe".as("chave"),
      $"parsed.protCTe.infProt.dhRecbto".as("infprot_dhrecbto"),
      $"parsed.protCTe.infProt.digVal".as("infprot_digval"),
      $"parsed.protCTe.infProt.nProt".as("infprot_nprot"),
      $"parsed.protCTe.infProt.tpAmb".as("infprot_tpamb"),
      $"parsed.protCTe.infProt.verAplic".as("infprot_veraplic"),
      $"parsed.protCTe.infProt.xMotivo".as("infprot_xmotivo"),
      $"parsed.GTVe.infCTeSupl.qrCodCTe".as("infctesupl_qrcodcte"),
      $"parsed.GTVe.infCte._Id".as("infCte_id"),
      $"parsed.GTVe.infCte._versao".as("infCte_versao"),
      $"parsed.GTVe.infCte.compl.xEmi".as("compl_xemi"),
      $"parsed.GTVe.infCte.compl.xObs".as("compl_xobs"),
      $"parsed.GTVe.infCte.dest.CNPJ".as("dest_cnpj"),
      $"parsed.GTVe.infCte.dest.IE".as("dest_ie"),
      $"parsed.GTVe.infCte.dest.enderDest.CEP".as("enderdest_cep"),
      $"parsed.GTVe.infCte.dest.enderDest.UF".as("enderdest_uf"),
      $"parsed.GTVe.infCte.dest.enderDest.cMun".as("enderdest_cmun"),
      $"parsed.GTVe.infCte.dest.enderDest.cPais".as("enderdest_cpais"),
      $"parsed.GTVe.infCte.dest.enderDest.nro".as("enderdest_nro"),
      $"parsed.GTVe.infCte.dest.enderDest.xBairro".as("enderdest_xbairro"),
      $"parsed.GTVe.infCte.dest.enderDest.xCpl".as("enderdest_xcpl"),
      $"parsed.GTVe.infCte.dest.enderDest.xLgr".as("enderdest_xlgr"),
      $"parsed.GTVe.infCte.dest.enderDest.xMun".as("enderdest_xmun"),
      $"parsed.GTVe.infCte.dest.enderDest.xPais".as("enderdest_xpais"),
      $"parsed.GTVe.infCte.dest.xNome".as("dest_xnome"),
      $"parsed.GTVe.infCte.destino.CEP".as("destino_cep"),
      $"parsed.GTVe.infCte.destino.UF".as("destino_uf"),
      $"parsed.GTVe.infCte.destino.cMun".as("destino_cmun"),
      $"parsed.GTVe.infCte.destino.nro".as("destino_nro"),
      $"parsed.GTVe.infCte.destino.xBairro".as("destino_xbairro"),
      $"parsed.GTVe.infCte.destino.xCpl".as("destino_xcpl"),
      $"parsed.GTVe.infCte.destino.xLgr".as("destino_xlgr"),
      $"parsed.GTVe.infCte.destino.xMun".as("destino_xmun"),
      $"parsed.GTVe.infCte.detGTV.infEspecie".as("detgtv_infespecie"),
      $"parsed.GTVe.infCte.detGTV.infVeiculo.RNTRC".as("infveiculo_rntrc"),
      $"parsed.GTVe.infCte.detGTV.infVeiculo.UF".as("infveiculo_uf"),
      $"parsed.GTVe.infCte.detGTV.infVeiculo.placa".as("infveiculo_placa"),
      $"parsed.GTVe.infCte.detGTV.qCarga".as("detgtv_qcarga"),
      $"parsed.GTVe.infCte.emit.CNPJ".as("emit_cnpj"),
      $"parsed.GTVe.infCte.emit.IE".as("emit_ie"),
      $"parsed.GTVe.infCte.emit.enderEmit.CEP".as("enderemit_cep"),
      $"parsed.GTVe.infCte.emit.enderEmit.UF".as("enderemit_uf"),
      $"parsed.GTVe.infCte.emit.enderEmit.cMun".as("enderemit_cmun"),
      $"parsed.GTVe.infCte.emit.enderEmit.fone".as("enderemit_fone"),
      $"parsed.GTVe.infCte.emit.enderEmit.nro".as("enderemit_nro"),
      $"parsed.GTVe.infCte.emit.enderEmit.xBairro".as("enderemit_xbairro"),
      $"parsed.GTVe.infCte.emit.enderEmit.xCpl".as("enderemit_xcpl"),
      $"parsed.GTVe.infCte.emit.enderEmit.xLgr".as("enderemit_xlgr"),
      $"parsed.GTVe.infCte.emit.enderEmit.xMun".as("enderemit_xmun"),
      $"parsed.GTVe.infCte.emit.xFant".as("emit_xfant"),
      $"parsed.GTVe.infCte.emit.xNome".as("emit_xnome"),
      $"parsed.GTVe.infCte.ide.CFOP".as("ide_cfop"),
      $"parsed.GTVe.infCte.ide.UFEnv".as("ide_ufenv"),
      $"parsed.GTVe.infCte.ide.cCT".as("ide_cct"),
      $"parsed.GTVe.infCte.ide.cDV".as("ide_cdv"),
      $"parsed.GTVe.infCte.ide.cMunEnv".as("ide_cmunenv"),
      $"parsed.GTVe.infCte.ide.cUF".as("ide_cuf"),
      $"parsed.GTVe.infCte.ide.dhChegadaDest".as("ide_dhchegadadest"),
      $"parsed.GTVe.infCte.ide.dhEmi".as("ide_dhemi"),
      $"parsed.GTVe.infCte.ide.dhSaidaOrig".as("ide_dhsaidaorig"),
      $"parsed.GTVe.infCte.ide.indIEToma".as("ide_indietoma"),
      $"parsed.GTVe.infCte.ide.mod".as("ide_mod"),
      $"parsed.GTVe.infCte.ide.modal".as("ide_modal"),
      $"parsed.GTVe.infCte.ide.nCT".as("ide_nct"),
      $"parsed.GTVe.infCte.ide.natOp".as("ide_natop"),
      $"parsed.GTVe.infCte.ide.serie".as("ide_serie"),
      $"parsed.GTVe.infCte.ide.toma.toma".as("toma_toma"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.CNPJ".as("tomaterceiro_cnpj"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.IE".as("tomaterceiro_ie"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.CEP".as("endertoma_cep"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.UF".as("endertoma_uf"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.cMun".as("endertoma_cmun"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.nro".as("endertoma_nro"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xBairro".as("endertoma_xbairro"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xCpl".as("endertoma_xcpl"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xLgr".as("endertoma_xlgr"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xMun".as("endertoma_xmun"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.toma".as("tomaterceiro_toma"),
      $"parsed.GTVe.infCte.ide.tomaTerceiro.xNome".as("tomaterceiro_xnome"),
      $"parsed.GTVe.infCte.ide.tpAmb".as("ide_tpamb"),
      $"parsed.GTVe.infCte.ide.tpCTe".as("ide_tpcte"),
      $"parsed.GTVe.infCte.ide.tpEmis".as("ide_tpemis"),
      $"parsed.GTVe.infCte.ide.tpImp".as("ide_tpimp"),
      $"parsed.GTVe.infCte.ide.tpServ".as("ide_tpserv"),
      $"parsed.GTVe.infCte.ide.verProc".as("ide_verproc"),
      $"parsed.GTVe.infCte.ide.xMunEnv".as("ide_xmunenv"),
      $"parsed.GTVe.infCte.infRespTec.CNPJ".as("infresptec_cnpj"),
      $"parsed.GTVe.infCte.infRespTec.email".as("infresptec_email"),
      $"parsed.GTVe.infCte.infRespTec.fone".as("infresptec_fone"),
      $"parsed.GTVe.infCte.infRespTec.xContato".as("infresptec_xcontato"),
      $"parsed.GTVe.infCte.origem.CEP".as("origem_cep"),
      $"parsed.GTVe.infCte.origem.UF".as("origem_uf"),
      $"parsed.GTVe.infCte.origem.cMun".as("origem_cmun"),
      $"parsed.GTVe.infCte.origem.nro".as("origem_nro"),
      $"parsed.GTVe.infCte.origem.xBairro".as("origem_xbairro"),
      $"parsed.GTVe.infCte.origem.xCpl".as("origem_xcpl"),
      $"parsed.GTVe.infCte.origem.xLgr".as("origem_xlgr"),
      $"parsed.GTVe.infCte.origem.xMun".as("origem_xmun"),
      $"parsed.GTVe.infCte.rem.CNPJ".as("rem_cnpj"),
      $"parsed.GTVe.infCte.rem.IE".as("rem_ie"),
      $"parsed.GTVe.infCte.rem.email".as("rem_email"),
      $"parsed.GTVe.infCte.rem.enderReme.CEP".as("enderreme_cep"),
      $"parsed.GTVe.infCte.rem.enderReme.UF".as("enderreme_uf"),
      $"parsed.GTVe.infCte.rem.enderReme.cMun".as("enderreme_cmun"),
      $"parsed.GTVe.infCte.rem.enderReme.cPais".as("enderreme_cpais"),
      $"parsed.GTVe.infCte.rem.enderReme.nro".as("enderreme_nro"),
      $"parsed.GTVe.infCte.rem.enderReme.xBairro".as("enderreme_xbairro"),
      $"parsed.GTVe.infCte.rem.enderReme.xCpl".as("enderreme_xcpl"),
      $"parsed.GTVe.infCte.rem.enderReme.xLgr".as("enderreme_xlgr"),
      $"parsed.GTVe.infCte.rem.enderReme.xMun".as("enderreme_xmun"),
      $"parsed.GTVe.infCte.rem.enderReme.xPais".as("enderreme_xpais"),
      $"parsed.GTVe.infCte.rem.fone".as("rem_fone"),
      $"parsed.GTVe.infCte.rem.xFant".as("rem_xfant"),
      $"parsed.GTVe.infCte.rem.xNome".as("rem_xnome")
    )
  }
}