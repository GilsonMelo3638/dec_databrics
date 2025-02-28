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
package DECCTeProcessor

import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

import java.time.LocalDateTime

object NF3eProcLegadoProcessor {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val anoInicio = 2024
  val anoFim = 2024
  val tipoDocumento = "nf3e"

  // Função para criar o esquema de forma modular

  import org.apache.spark.sql.types._

  def createSchema(): StructType = {
    new StructType()
      .add("NF3e", new StructType()
        .add("infNF3e", new StructType()
          .add("NFdet", new StructType()
            .add("det", ArrayType(new StructType()
              .add("_nItem", StringType, true)
              .add("detItem", new StructType()
                .add("imposto", new StructType()
                  .add("COFINS", new StructType()
                    .add("CST", StringType, true)
                    .add("pCOFINS", DoubleType, true)
                    .add("vBC", DoubleType, true)
                    .add("vCOFINS", DoubleType, true))
                  .add("COFINSEfet", new StructType()
                    .add("pCOFINSEfet", DoubleType, true)
                    .add("vBCCOFINSEfet", DoubleType, true)
                    .add("vCOFINSEfet", DoubleType, true))
                  .add("ICMS00", new StructType()
                    .add("CST", StringType, true)
                    .add("pICMS", DoubleType, true)
                    .add("vBC", DoubleType, true)
                    .add("vICMS", DoubleType, true))
                  .add("ICMS40", new StructType()
                    .add("CST", StringType, true)
                    .add("cBenef", StringType, true)
                    .add("vICMSDeson", DoubleType, true))
                  .add("ICMS90", new StructType()
                    .add("CST", StringType, true))
                  .add("PIS", new StructType()
                    .add("CST", StringType, true)
                    .add("pPIS", DoubleType, true)
                    .add("vBC", DoubleType, true)
                    .add("vPIS", DoubleType, true))
                  .add("PISEfet", new StructType()
                    .add("pPISEfet", DoubleType, true)
                    .add("vBCPISEfet", DoubleType, true)
                    .add("vPISEfet", DoubleType, true))
                  .add("indSemCST", StringType, true)
                  .add("retTrib", new StructType()
                    .add("vBCIRRF", DoubleType, true)
                    .add("vIRRF", DoubleType, true)
                    .add("vRetCSLL", DoubleType, true)
                    .add("vRetCofins", DoubleType, true)
                    .add("vRetPIS", DoubleType, true))))
              .add("prod", new StructType()
                .add("CFOP", StringType, true)
                .add("cClass", StringType, true)
                .add("cProd", StringType, true)
                .add("gMedicao", new StructType()
                  .add("gMedida", new StructType()
                    .add("cPosTarif", StringType, true)
                    .add("pPerdaTran", DoubleType, true)
                    .add("tpGrMed", StringType, true)
                    .add("uMed", StringType, true)
                    .add("vConst", DoubleType, true)
                    .add("vMed", DoubleType, true)
                    .add("vMedAnt", DoubleType, true)
                    .add("vMedAtu", DoubleType, true)
                    .add("vMedPerdaTran", DoubleType, true))
                  .add("nContrat", StringType, true)
                  .add("nMed", StringType, true)
                  .add("tpMotNaoLeitura", StringType, true))
                .add("indOrigemQtd", StringType, true)
                .add("qFaturada", DoubleType, true)
                .add("uMed", StringType, true)
                .add("vItem", DoubleType, true)
                .add("vProd", DoubleType, true)
                .add("xProd", StringType, true))))
            .add("_Id", StringType, true)
            .add("_versao", StringType, true))
          .add("acessante", new StructType()
            .add("idAcesso", StringType, true)
            .add("idCodCliente", StringType, true)
            .add("latGPS", DoubleType, true)
            .add("longGPS", DoubleType, true)
            .add("tpAcesso", StringType, true)
            .add("tpClasse", StringType, true)
            .add("tpFase", StringType, true)
            .add("tpGrpTensao", StringType, true)
            .add("tpModTar", StringType, true)
            .add("tpSubClasse", StringType, true)
            .add("xNomeUC", StringType, true))
          .add("dest", new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true)
            .add("IE", StringType, true)
            .add("NB", StringType, true)
            .add("cNIS", StringType, true)
            .add("enderDest", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true))
            .add("idOutros", StringType, true)
            .add("indIEDest", StringType, true)
            .add("xNome", StringType, true))
          .add("emit", new StructType()
            .add("CNPJ", StringType, true)
            .add("IE", StringType, true)
            .add("enderEmit", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("fone", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true))
            .add("xFant", StringType, true)
            .add("xNome", StringType, true))
          .add("gANEEL", new StructType()
            .add("gHistFat", ArrayType(new StructType()
              .add("gGrandFat", ArrayType(new StructType()
                .add("CompetFat", StringType, true)
                .add("qtdDias", StringType, true)
                .add("uMed", StringType, true)
                .add("vFat", DoubleType, true)), true)
              .add("xGrandFat", StringType, true)), true))
          .add("gFat", new StructType()
            .add("CompetFat", StringType, true)
            .add("codBarras", StringType, true)
            .add("codDebAuto", StringType, true)
            .add("dApresFat", StringType, true)
            .add("dProxLeitura", StringType, true)
            .add("dVencFat", StringType, true)
            .add("gPIX", new StructType()
              .add("urlQRCodePIX", StringType, true))
            .add("nFat", StringType, true))
          .add("gGrContrat", ArrayType(new StructType()
            .add("_nContrat", StringType, true)
            .add("qUnidContrat", DoubleType, true)
            .add("tpGrContrat", StringType, true)
            .add("tpPosTar", StringType, true)), true)
          .add("gMed", new StructType()
            .add("_nMed", StringType, true)
            .add("dMedAnt", StringType, true)
            .add("dMedAtu", StringType, true)
            .add("idMedidor", StringType, true))
          .add("gRespTec", new StructType()
            .add("CNPJ", StringType, true)
            .add("email", StringType, true)
            .add("fone", StringType, true)
            .add("xContato", StringType, true))
          .add("gSub", new StructType()
            .add("chNF3e", StringType, true)
            .add("gNF", new StructType()
              .add("CNPJ", StringType, true)
              .add("CompetApur", StringType, true)
              .add("CompetEmis", StringType, true)
              .add("hash115", StringType, true)
              .add("nNF", StringType, true)
              .add("serie", StringType, true))
            .add("motSub", StringType, true))
          .add("ide", new StructType()
            .add("cDV", StringType, true)
            .add("cMunFG", StringType, true)
            .add("cNF", StringType, true)
            .add("cUF", StringType, true)
            .add("dhCont", StringType, true)
            .add("dhEmi", StringType, true)
            .add("finNF3e", StringType, true)
            .add("mod", StringType, true)
            .add("nNF", StringType, true)
            .add("nSiteAutoriz", StringType, true)
            .add("serie", StringType, true)
            .add("tpAmb", StringType, true)
            .add("tpEmis", StringType, true)
            .add("verProc", StringType, true)
            .add("xJust", StringType, true))
          .add("total", new StructType()
            .add("ICMSTot", new StructType()
              .add("vBC", DoubleType, true)
              .add("vBCST", DoubleType, true)
              .add("vFCP", DoubleType, true)
              .add("vFCPST", DoubleType, true)
              .add("vICMS", DoubleType, true)
              .add("vICMSDeson", DoubleType, true)
              .add("vST", DoubleType, true))
            .add("vCOFINS", DoubleType, true)
            .add("vCOFINSEfet", DoubleType, true)
            .add("vNF", DoubleType, true)
            .add("vPIS", DoubleType, true)
            .add("vPISEfet", DoubleType, true)
            .add("vProd", DoubleType, true)
            .add("vRetTribTot", new StructType()
              .add("vIRRF", DoubleType, true)
              .add("vRetCSLL", DoubleType, true)
              .add("vRetCofins", DoubleType, true)
              .add("vRetPIS", DoubleType, true))))
        .add("infNF3eSupl", new StructType()
          .add("qrCodNF3e", StringType, true)))
      .add("_dhConexao", StringType, true)
      .add("_ipTransmissor", StringType, true)
      .add("_nPortaCon", StringType, true)
      .add("_versao", StringType, true)
      .add("_xmlns", StringType, true)
      .add("protNF3e", new StructType()
        .add("_versao", StringType, true)
        .add("infProt", new StructType()
          .add("_Id", StringType, true)
          .add("cStat", StringType, true)
          .add("chNF3e", StringType, true)
          .add("dhRecbto", StringType, true)
          .add("digVal", StringType, true)
          .add("nProt", StringType, true)
          .add("tpAmb", StringType, true)
          .add("verAplic", StringType, true)
          .add("xMotivo", StringType, true)))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val schema = createSchema()
    // Lista de anos com base nas variáveis externas
    val anoList = (anoInicio to anoFim).map(_.toString).toList

    anoList.foreach { ano =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/$tipoDocumento/202307_202501"

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

      // Redistribuir os dados para 40 partições
      val repartitionedDF = selectedDFComParticao.repartition(40)

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

//NF3eProcLegadoProcessor.main(Array())
