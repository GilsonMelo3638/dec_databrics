//scp "C:\dec\target\DecInfNFePrata-0.0.1-SNAPSHOT.jar"  gamelo@10.69.22.71:src/main/scala/DecInfNFePrata-0.0.1-SNAPSHOT.jar
//hdfs dfs -put -f /export/home/gamelo/src/main/scala/DecInfNFePrata-0.0.1-SNAPSHOT.jar /app/dec
//hdfs dfs -ls /app/dec
// hdfs dfs -rm -skipTrash /app/dec/DecInfNFePrata-0.0.1-SNAPSHOT.jar
// spark-submit \
//  --class DECJob.InfNFeProcessor \
//  --master yarn \
//  --deploy-mode cluster \
//  --num-executors 20 \
//  --executor-memory 4G \
//  --executor-cores 2 \
//  --conf "spark.sql.parquet.writeLegacyFormat=true" \
//  --conf "spark.sql.debug.maxToStringFields=100" \
//  --conf "spark.executor.memoryOverhead=1024" \
//  --conf "spark.network.timeout=800s" \
//  --conf "spark.yarn.executor.memoryOverhead=4096" \
//  --conf "spark.shuffle.service.enabled=true" \
//  --conf "spark.dynamicAllocation.enabled=true" \
//  --conf "spark.dynamicAllocation.minExecutors=10" \
//  --conf "spark.dynamicAllocation.maxExecutors=40" \
//  --packages com.databricks:spark-xml_2.12:0.13.0 \
//  hdfs://sepladbigdata/app/dec/DecInfNFePrata-0.0.1-SNAPSHOT.jar
package DECMDFeProcessor

import Schemas.MDFeSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object MDFeProcLegadoMensalProcessor {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val ano = 2025
  val mesInicio = 2
  val mesFim = 2
  val tipoDocumento = "mdfe"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInMDFe").enableHiveSupport().getOrCreate()
    import spark.implicits._
    // Obter o esquema da classe CTeOSSchema
    val schema = MDFeSchema.createSchema()
    // Lista de meses com base nas variáveis externas
    val anoMesList = (mesInicio to mesFim).map { month =>
      f"$ano${month}%02d"
    }.toList

    anoMesList.foreach { anoMes =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/$tipoDocumento/$anoMes"

      // Registrar o horário de início da iteração
      val startTime = LocalDateTime.now()
      println(s"Início da iteração para $ano: $startTime")
      println(s"Lendo dados do caminho: $parquetPath")

      // 1. Carrega o arquivo Parquet
      val parquetDF = spark.read.parquet(parquetPath)

      // 2. Seleciona as colunas e filtra MODELO = 64
      val xmlDF = parquetDF
        .filter($"NSU" < 1000000000) // Aplica o filtro antes da seleção
        .select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSU".cast("string").as("NSU"),
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR"
        )
      xmlDF.show()
      // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
      val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))
      //     parsedDF.printSchema()

      // 4. Seleciona os campos desejados
      val selectedDF = parsedDF.select(
        $"NSU",
        date_format(to_timestamp($"DHPROC", "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH").as("DHPROC_FORMATADO"),
        $"DHEMI",
        $"IP_TRANSMISSOR",
        $"parsed.MDFe.infMDFe._Id".as("infmdfe_id"),
        $"parsed.MDFe.infMDFe._versao".as("infmdfe_versao"),
        $"parsed.MDFe.infMDFe.autXML".as("infmdfe_autxml"), // Array intacto
        $"parsed.MDFe.infMDFe.emit.CNPJ".as("emit_cnpj"),
        $"parsed.MDFe.infMDFe.emit.CPF".as("emit_cpf"),
        $"parsed.MDFe.infMDFe.emit.IE".as("emit_ie"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.CEP".as("enderemit_cep"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.UF".as("enderemit_uf"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.cMun".as("enderemit_cmun"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.email".as("enderemit_email"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.fone".as("enderemit_fone"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.nro".as("enderemit_nro"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.xBairro".as("enderemit_xbairro"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.xCpl".as("enderemit_xcpl"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.xLgr".as("enderemit_xlgr"),
        $"parsed.MDFe.infMDFe.emit.enderEmit.xMun".as("enderemit_xmun"),
        $"parsed.MDFe.infMDFe.emit.xFant".as("emit_xfant"),
        $"parsed.MDFe.infMDFe.emit.xNome".as("emit_xnome"),
        $"parsed.MDFe.infMDFe.ide.UFFim".as("ide_uffim"),
        $"parsed.MDFe.infMDFe.ide.UFIni".as("ide_ufini"),
        $"parsed.MDFe.infMDFe.ide.cDV".as("ide_cdv"),
        $"parsed.MDFe.infMDFe.ide.cMDF".as("ide_cmdf"),
        $"parsed.MDFe.infMDFe.ide.cUF".as("ide_cuf"),
        $"parsed.MDFe.infMDFe.ide.dhEmi".as("ide_dhemi"),
        $"parsed.MDFe.infMDFe.ide.dhIniViagem".as("ide_dhiniviagem"),
        $"parsed.MDFe.infMDFe.ide.indCanalVerde".as("ide_indcanalverde"),
        $"parsed.MDFe.infMDFe.ide.infMunCarrega".as("ide_infmuncarrega"), // Array intacto
        $"parsed.MDFe.infMDFe.ide.infPercurso".as("ide_infpercurso"), // Array intacto
        $"parsed.MDFe.infMDFe.ide.mod".as("ide_mod"),
        $"parsed.MDFe.infMDFe.ide.modal".as("ide_modal"),
        $"parsed.MDFe.infMDFe.ide.nMDF".as("ide_nmdf"),
        $"parsed.MDFe.infMDFe.ide.procEmi".as("ide_procemi"),
        $"parsed.MDFe.infMDFe.ide.serie".as("ide_serie"),
        $"parsed.MDFe.infMDFe.ide.tpAmb".as("ide_tpamb"),
        $"parsed.MDFe.infMDFe.ide.tpEmis".as("ide_tpemis"),
        $"parsed.MDFe.infMDFe.ide.tpEmit".as("ide_tpemit"),
        $"parsed.MDFe.infMDFe.ide.tpTransp".as("ide_tptransp"),
        $"parsed.MDFe.infMDFe.ide.verProc".as("ide_verproc"),
        $"parsed.MDFe.infMDFe.infAdic.infAdFisco".as("infadic_infadfisco"),
        $"parsed.MDFe.infMDFe.infAdic.infCpl".as("infadic_infcpl"),
        $"parsed.MDFe.infMDFe.infDoc.infMunDescarga".as("infdoc_infmundescarga"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal._versaoModal".as("infmodal_versaomodal"),
        $"parsed.MDFe.infMDFe.infModal.aereo.cAerDes".as("aereo_caerdes"),
        $"parsed.MDFe.infMDFe.infModal.aereo.cAerEmb".as("aereo_caeremb"),
        $"parsed.MDFe.infMDFe.infModal.aereo.dVoo".as("aereo_dvoo"),
        $"parsed.MDFe.infMDFe.infModal.aereo.matr".as("aereo_matr"),
        $"parsed.MDFe.infMDFe.infModal.aereo.nVoo".as("aereo_nvoo"),
        $"parsed.MDFe.infMDFe.infModal.aereo.nac".as("aereo_nac"),
        $"parsed.MDFe.infMDFe.infModal.aquav.CNPJAgeNav".as("aquav_cnpjagenav"),
        $"parsed.MDFe.infMDFe.infModal.aquav.cEmbar".as("aquav_cembar"),
        $"parsed.MDFe.infMDFe.infModal.aquav.cPrtDest".as("aquav_cprtdest"),
        $"parsed.MDFe.infMDFe.infModal.aquav.cPrtEmb".as("aquav_cprtemb"),
        $"parsed.MDFe.infMDFe.infModal.aquav.infTermCarreg.cTermCarreg".as("inftermcarreg_ctermcarreg"),
        $"parsed.MDFe.infMDFe.infModal.aquav.infTermCarreg.xTermCarreg".as("inftermcarreg_xtermcarreg"),
        $"parsed.MDFe.infMDFe.infModal.aquav.infTermDescarreg.cTermDescarreg".as("inftermdescarreg_ctermdescarreg"),
        $"parsed.MDFe.infMDFe.infModal.aquav.infTermDescarreg.xTermDescarreg".as("inftermdescarreg_xtermdescarreg"),
        $"parsed.MDFe.infMDFe.infModal.aquav.irin".as("aquav_irin"),
        $"parsed.MDFe.infMDFe.infModal.aquav.nViag".as("aquav_nviag"),
        $"parsed.MDFe.infMDFe.infModal.aquav.prtTrans".as("aquav_prttrans"),
        $"parsed.MDFe.infMDFe.infModal.aquav.tpEmb".as("aquav_tpemb"),
        $"parsed.MDFe.infMDFe.infModal.aquav.tpNav".as("aquav_tpnav"),
        $"parsed.MDFe.infMDFe.infModal.aquav.xEmbar".as("aquav_xembar"),
        $"parsed.MDFe.infMDFe.infModal.ferrov.trem.qVag".as("trem_qvag"),
        $"parsed.MDFe.infMDFe.infModal.ferrov.trem.xDest".as("trem_xdest"),
        $"parsed.MDFe.infMDFe.infModal.ferrov.trem.xOri".as("trem_xori"),
        $"parsed.MDFe.infMDFe.infModal.ferrov.trem.xPref".as("trem_xpref"),
        $"parsed.MDFe.infMDFe.infModal.ferrov.vag".as("ferrov_vag"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.CIOT".as("rodo_ciot"),
        $"parsed.MDFe.infMDFe.infModal.rodo.RNTRC".as("rodo_rntrc"),
        $"parsed.MDFe.infMDFe.infModal.rodo.codAgPorto".as("rodo_codagporto"),
        $"parsed.MDFe.infMDFe.infModal.rodo.infANTT.RNTRC".as("infantt_rntrc"),
        $"parsed.MDFe.infMDFe.infModal.rodo.infANTT.infCIOT".as("infantt_infciot"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.infANTT.infContratante".as("infantt_infcontratante"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.infANTT.infPag".as("infantt_infpag"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.valePed.categCombVeic".as("valeped_categcombveic"),
        $"parsed.MDFe.infMDFe.infModal.rodo.valePed.disp".as("valeped_disp"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.lacRodo".as("rodo_lacrodo"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.cInt".as("veicprincipal_cint"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.capKG".as("veicprincipal_capkg"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.capM3".as("veicprincipal_capm3"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.condutor.CPF".as("condutor_cpf"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.condutor.xNome".as("condutor_xnome"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.placa".as("veicprincipal_placa"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.prop.RNTRC".as("veicprincipal_rntrc"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicPrincipal.tara".as("veicprincipal_tara"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicReboque".as("rodo_veicreboque"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.RENAVAM".as("veictracao_renavam"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.UF".as("veictracao_uf"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.cInt".as("veictracao_cint"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.capKG".as("veictracao_capkg"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.capM3".as("veictracao_capm3"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.condutor".as("veictracao_condutor"), // Array intacto
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.placa".as("veictracao_placa"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.CNPJ".as("prop_cnpj"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.CPF".as("prop_cpf"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.IE".as("prop_ie"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.RNTRC".as("veictracao_rntrc"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.UF".as("prop_uf"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.tpProp".as("prop_tpprop"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.prop.xNome".as("prop_xnome"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.tara".as("veictracao_tara"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.tpCar".as("veictracao_tpcar"),
        $"parsed.MDFe.infMDFe.infModal.rodo.veicTracao.tpRod".as("veictracao_tprod"),
        $"parsed.MDFe.infMDFe.infRespTec.CNPJ".as("infresptec_cnpj"),
        $"parsed.MDFe.infMDFe.infRespTec.email".as("infresptec_email"),
        $"parsed.MDFe.infMDFe.infRespTec.fone".as("infresptec_fone"),
        $"parsed.MDFe.infMDFe.infRespTec.hashCSRT".as("infresptec_hashcsrt"),
        $"parsed.MDFe.infMDFe.infRespTec.idCSRT".as("infresptec_idcsrt"),
        $"parsed.MDFe.infMDFe.infRespTec.xContato".as("infresptec_xcontato"),
        $"parsed.MDFe.infMDFe.lacres".as("infmdfe_lacres"), // Array intacto
        $"parsed.MDFe.infMDFe.prodPred.NCM".as("prodpred_ncm"),
        $"parsed.MDFe.infMDFe.prodPred.cEAN".as("prodpred_cean"),
        $"parsed.MDFe.infMDFe.prodPred.infLotacao.infLocalCarrega.CEP".as("inflocalcarrega_cep"),
        $"parsed.MDFe.infMDFe.prodPred.infLotacao.infLocalCarrega.latitude".as("inflocalcarrega_latitude"),
        $"parsed.MDFe.infMDFe.prodPred.infLotacao.infLocalCarrega.longitude".as("inflocalcarrega_longitude"),
        $"parsed.MDFe.infMDFe.prodPred.infLotacao.infLocalDescarrega.CEP".as("inflocaldescarrega_cep"),
        $"parsed.MDFe.infMDFe.prodPred.infLotacao.infLocalDescarrega.latitude".as("inflocaldescarrega_latitude"),
        $"parsed.MDFe.infMDFe.prodPred.infLotacao.infLocalDescarrega.longitude".as("inflocaldescarrega_longitude"),
        $"parsed.MDFe.infMDFe.prodPred.tpCarga".as("prodpred_tpcarga"),
        $"parsed.MDFe.infMDFe.prodPred.xProd".as("prodpred_xprod"),
        $"parsed.MDFe.infMDFe.seg".as("infmdfe_seg"), // Array intacto
        $"parsed.MDFe.infMDFe.tot.cUnid".as("tot_cunid"),
        $"parsed.MDFe.infMDFe.tot.qCT".as("tot_qct"),
        $"parsed.MDFe.infMDFe.tot.qCTe".as("tot_qcte"),
        $"parsed.MDFe.infMDFe.tot.qCarga".as("tot_qcarga"),
        $"parsed.MDFe.infMDFe.tot.qMDFe".as("tot_qmdfe"),
        $"parsed.MDFe.infMDFe.tot.qNF".as("tot_qnf"),
        $"parsed.MDFe.infMDFe.tot.qNFe".as("tot_qnfe"),
        $"parsed.MDFe.infMDFe.tot.vCarga".as("tot_vcarga"),
        $"parsed.MDFe.infMDFeSupl.qrCodMDFe".as("infmdfesupl_qrcodmdfe"),
        $"parsed._dhConexao".as("dhconexao"),
        $"parsed._ipTransmissor".as("iptransmissor"),
        $"parsed._nPortaCon".as("nportacon"),
        $"parsed._versao".as("versao"),
        $"parsed._xmlns".as("xmlns"),
        $"parsed.protMDFe._versao".as("protmdfe_mdfe_versao"),
        $"parsed.protMDFe.infProt._Id".as("protmdfe_mdfe_infprot_id"),
        $"parsed.protMDFe.infProt.cStat".as("protmdfe_mdfe_infprot_cstat"),
        $"parsed.protMDFe.infProt.chMDFe".as("chave"),
        $"parsed.protMDFe.infProt.dhRecbto".as("protmdfe_dhrecbto"),
        $"parsed.protMDFe.infProt.digVal".as("protmdfe_digval"),
        $"parsed.protMDFe.infProt.nProt".as("protmdfemdfe_nprot"),
        $"parsed.protMDFe.infProt.tpAmb".as("protmdfemdfe_tpamb"),
        $"parsed.protMDFe.infProt.verAplic".as("protmdfemdfe_veraplic"),
        $"parsed.protMDFe.infProt.xMotivo".as("protmdfemdfe_xmotivo")
      )
      // Criando uma nova coluna 'chave_particao' extraindo os dígitos 3 a 6 da coluna 'CHAVE'
      val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

      // Imprimir no console as variações e a contagem de 'chave_particao'
      val chaveParticaoContagem = selectedDFComParticao
        .groupBy("chave_particao")
        .agg(count("chave").alias("contagem_chaves"))
        .orderBy("chave_particao")

      // Coletar os dados para exibição no console
      chaveParticaoContagem.collect().foreach { row =>
        println(s"Variação: ${row.getAs[String]("chave_particao")}, Contagem: ${row.getAs[Long]("contagem_chaves")}")
      }

      // Redistribuir os dados para 40 partições
      val repartitionedDF = selectedDFComParticao.repartition(1)

      // Escrever os dados particionados
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
        .partitionBy("chave_particao") // Garante a separação por partição
        .save("/datalake/prata/sources/dbms/dec/mdfe/MDFe")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")
    }
  }
}

//MDFeProcLegadoMensalProcessor.main(Array())
