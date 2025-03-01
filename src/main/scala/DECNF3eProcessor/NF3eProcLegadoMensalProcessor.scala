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
package DECNF3eProcessor

import Schemas.NF3eSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object NF3eProcLegadoMensalProcessor {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val ano = 2025
  val mesInicio = 2
  val mesFim = 2
  val tipoDocumento = "nf3e"

  // Função para criar o esquema de forma modular

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInfNF3e").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe CTeOSSchema
    val schema = NF3eSchema.createSchema()
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
        $"parsed.NF3e.infNF3e.NFdet.det".as("nfdet_det"), // Array intacto
        $"parsed.NF3e.infNF3e.NFdet._Id".as("nfdet_id"),
        $"parsed.NF3e.infNF3e.NFdet._versao".as("nfdet_versao"),
        $"parsed.NF3e.infNF3e.acessante.idAcesso".as("acessante_idacesso"),
        $"parsed.NF3e.infNF3e.acessante.idCodCliente".as("acessante_idcodcliente"),
        $"parsed.NF3e.infNF3e.acessante.latGPS".as("acessante_latgps"),
        $"parsed.NF3e.infNF3e.acessante.longGPS".as("acessante_longgps"),
        $"parsed.NF3e.infNF3e.acessante.tpAcesso".as("acessante_tpacesso"),
        $"parsed.NF3e.infNF3e.acessante.tpClasse".as("acessante_tpclasse"),
        $"parsed.NF3e.infNF3e.acessante.tpFase".as("acessante_tpfase"),
        $"parsed.NF3e.infNF3e.acessante.tpGrpTensao".as("acessante_tpgrptensao"),
        $"parsed.NF3e.infNF3e.acessante.tpModTar".as("acessante_tpmodtar"),
        $"parsed.NF3e.infNF3e.acessante.tpSubClasse".as("acessante_tpsubclasse"),
        $"parsed.NF3e.infNF3e.acessante.xNomeUC".as("acessante_xnomeuc"),
        $"parsed.NF3e.infNF3e.dest.CNPJ".as("dest_cnpj"),
        $"parsed.NF3e.infNF3e.dest.CPF".as("dest_cpf"),
        $"parsed.NF3e.infNF3e.dest.IE".as("dest_ie"),
        $"parsed.NF3e.infNF3e.dest.NB".as("dest_nb"),
        $"parsed.NF3e.infNF3e.dest.cNIS".as("dest_cnis"),
        $"parsed.NF3e.infNF3e.dest.enderDest.CEP".as("enderdest_cep"),
        $"parsed.NF3e.infNF3e.dest.enderDest.UF".as("enderdest_uf"),
        $"parsed.NF3e.infNF3e.dest.enderDest.cMun".as("enderdest_cmun"),
        $"parsed.NF3e.infNF3e.dest.enderDest.nro".as("enderdest_nro"),
        $"parsed.NF3e.infNF3e.dest.enderDest.xBairro".as("enderdest_xbairro"),
        $"parsed.NF3e.infNF3e.dest.enderDest.xCpl".as("enderdest_xcpl"),
        $"parsed.NF3e.infNF3e.dest.enderDest.xLgr".as("enderdest_xlgr"),
        $"parsed.NF3e.infNF3e.dest.enderDest.xMun".as("enderdest_xmun"),
        $"parsed.NF3e.infNF3e.dest.idOutros".as("dest_idoutros"),
        $"parsed.NF3e.infNF3e.dest.indIEDest".as("dest_indiedest"),
        $"parsed.NF3e.infNF3e.dest.xNome".as("dest_xnome"),
        $"parsed.NF3e.infNF3e.emit.CNPJ".as("emit_cnpj"),
        $"parsed.NF3e.infNF3e.emit.IE".as("emit_ie"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.CEP".as("enderemit_cep"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.UF".as("enderemit_uf"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.cMun".as("enderemit_cmun"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.fone".as("enderemit_fone"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.nro".as("enderemit_nro"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.xBairro".as("enderemit_xbairro"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.xCpl".as("enderemit_xcpl"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.xLgr".as("enderemit_xlgr"),
        $"parsed.NF3e.infNF3e.emit.enderEmit.xMun".as("enderemit_xmun"),
        $"parsed.NF3e.infNF3e.emit.xFant".as("emit_xfant"),
        $"parsed.NF3e.infNF3e.emit.xNome".as("emit_xnome"),
        $"parsed.NF3e.infNF3e.gANEEL.gHistFat".as("ganeel_ghistfat"), // Array intacto
        $"parsed.NF3e.infNF3e.gFat.CompetFat".as("gfat_competfat"),
        $"parsed.NF3e.infNF3e.gFat.codBarras".as("gfat_codbarras"),
        $"parsed.NF3e.infNF3e.gFat.codDebAuto".as("gfat_coddebauto"),
        $"parsed.NF3e.infNF3e.gFat.dApresFat".as("gfat_dapresfat"),
        $"parsed.NF3e.infNF3e.gFat.dProxLeitura".as("gfat_dproxleitura"),
        $"parsed.NF3e.infNF3e.gFat.dVencFat".as("gfat_dvencfat"),
        $"parsed.NF3e.infNF3e.gFat.gPIX.urlQRCodePIX".as("gpix_urlqrcodepix"),
        $"parsed.NF3e.infNF3e.gFat.nFat".as("gfat_nfat"),
        $"parsed.NF3e.infNF3e.gGrContrat".as("ggrcontrat"), // Array intacto
        $"parsed.NF3e.infNF3e.gMed._nMed".as("gmed_nmed"),
        $"parsed.NF3e.infNF3e.gMed.dMedAnt".as("gmed_dmedant"),
        $"parsed.NF3e.infNF3e.gMed.dMedAtu".as("gmed_dmedatu"),
        $"parsed.NF3e.infNF3e.gMed.idMedidor".as("gmed_idmedidor"),
        $"parsed.NF3e.infNF3e.gRespTec.CNPJ".as("gresptec_cnpj"),
        $"parsed.NF3e.infNF3e.gRespTec.email".as("gresptec_email"),
        $"parsed.NF3e.infNF3e.gRespTec.fone".as("gresptec_fone"),
        $"parsed.NF3e.infNF3e.gRespTec.xContato".as("gresptec_xcontato"),
        $"parsed.NF3e.infNF3e.gSub.chNF3e".as("gsub_chnf3e"),
        $"parsed.NF3e.infNF3e.gSub.gNF.CNPJ".as("gnf_cnpj"),
        $"parsed.NF3e.infNF3e.gSub.gNF.CompetApur".as("gnf_competapur"),
        $"parsed.NF3e.infNF3e.gSub.gNF.CompetEmis".as("gnf_competemis"),
        $"parsed.NF3e.infNF3e.gSub.gNF.hash115".as("gnf_hash115"),
        $"parsed.NF3e.infNF3e.gSub.gNF.nNF".as("gnf_nnf"),
        $"parsed.NF3e.infNF3e.gSub.gNF.serie".as("gnf_serie"),
        $"parsed.NF3e.infNF3e.gSub.motSub".as("gsub_motsub"),
        $"parsed.NF3e.infNF3e.ide.cDV".as("ide_cdv"),
        $"parsed.NF3e.infNF3e.ide.cMunFG".as("ide_cmunfg"),
        $"parsed.NF3e.infNF3e.ide.cNF".as("ide_cnf"),
        $"parsed.NF3e.infNF3e.ide.cUF".as("ide_cuf"),
        $"parsed.NF3e.infNF3e.ide.dhCont".as("ide_dhcont"),
        $"parsed.NF3e.infNF3e.ide.dhEmi".as("ide_dhemi"),
        $"parsed.NF3e.infNF3e.ide.finNF3e".as("ide_finnf3e"),
        $"parsed.NF3e.infNF3e.ide.mod".as("ide_mod"),
        $"parsed.NF3e.infNF3e.ide.nNF".as("ide_nnf"),
        $"parsed.NF3e.infNF3e.ide.nSiteAutoriz".as("ide_nsiteautoriz"),
        $"parsed.NF3e.infNF3e.ide.serie".as("ide_serie"),
        $"parsed.NF3e.infNF3e.ide.tpAmb".as("ide_tpamb"),
        $"parsed.NF3e.infNF3e.ide.tpEmis".as("ide_tpemis"),
        $"parsed.NF3e.infNF3e.ide.verProc".as("ide_verproc"),
        $"parsed.NF3e.infNF3e.ide.xJust".as("ide_xjust"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vBC".as("icmstot_vbc"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vBCST".as("icmstot_vbcst"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vFCP".as("icmstot_vfcp"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vFCPST".as("icmstot_vfcpst"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vICMS".as("icmstot_vicms"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vICMSDeson".as("icmstot_vicmsdeson"),
        $"parsed.NF3e.infNF3e.total.ICMSTot.vST".as("icmstot_vst"),
        $"parsed.NF3e.infNF3e.total.vCOFINS".as("total_vcofins"),
        $"parsed.NF3e.infNF3e.total.vCOFINSEfet".as("total_vcofinsefet"),
        $"parsed.NF3e.infNF3e.total.vNF".as("total_vnf"),
        $"parsed.NF3e.infNF3e.total.vPIS".as("total_vpis"),
        $"parsed.NF3e.infNF3e.total.vPISEfet".as("total_vpisefet"),
        $"parsed.NF3e.infNF3e.total.vProd".as("total_vprod"),
        $"parsed.NF3e.infNF3e.total.vRetTribTot.vIRRF".as("vrettribtot_virrf"),
        $"parsed.NF3e.infNF3e.total.vRetTribTot.vRetCSLL".as("vrettribtot_vretcsll"),
        $"parsed.NF3e.infNF3e.total.vRetTribTot.vRetCofins".as("vrettribtot_vretcofins"),
        $"parsed.NF3e.infNF3e.total.vRetTribTot.vRetPIS".as("vrettribtot_vretpis"),
        $"parsed.NF3e.infNF3eSupl.qrCodNF3e".as("infnf3esupl_qrcodnf3e"),
        $"parsed._dhConexao".as("dhconexao"),
        $"parsed._ipTransmissor".as("iptransmissor"),
        $"parsed._nPortaCon".as("nportacon"),
        $"parsed._versao".as("versao"),
        $"parsed._xmlns".as("xmlns"),
        $"parsed.protNF3e._versao".as("protnf3e_versao"),
        $"parsed.protNF3e.infProt._Id".as("infprot_id"),
        $"parsed.protNF3e.infProt.cStat".as("infprot_cstat"),
        $"parsed.protNF3e.infProt.chNF3e".as("chave"),
        $"parsed.protNF3e.infProt.dhRecbto".as("infprot_dhrecbto"),
        $"parsed.protNF3e.infProt.digVal".as("infprot_digval"),
        $"parsed.protNF3e.infProt.nProt".as("infprot_nprot"),
        $"parsed.protNF3e.infProt.tpAmb".as("infprot_tpamb"),
        $"parsed.protNF3e.infProt.verAplic".as("infprot_veraplic"),
        $"parsed.protNF3e.infProt.xMotivo".as("infprot_xmotivo"))
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

      // Redistribuir os dados para 1 partições
      val repartitionedDF = selectedDFComParticao.repartition(1)

      // Escrever os dados particionados
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
        .partitionBy("chave_particao") // Garante a separação por partição
        .save("/datalake/prata/sources/dbms/dec/nf3e/nf3e")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")
    }
  }
}

//NF3eProcLegadoMensalProcessor.main(Array())
