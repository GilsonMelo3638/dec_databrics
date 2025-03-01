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
package DECSTeOSProcessor

import Schemas.CTeOSSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object CTeOSLegadoMensalProcessor {

  // Variáveis externas para o intervalo de meses e ano de processamento
  val ano = 2025
  val mesInicio = 1
  val mesFim = 2
  val tipoDocumento = "cte"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInfCTeOS").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe CTeOSSchema
    val schema = CTeOSSchema.createSchema() // Lista de anos com base nas variáveis externas
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

      // 2. Seleciona as colunas e filtra MODELO = 67
      val xmlDF = parquetDF
        .filter($"MODELO" === 67) // Filtra onde MODELO é igual a 64
        .select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSUSVD".cast("string").as("NSUSVD"),
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR",
          $"MODELO",
          $"TPEMIS"
        )
      xmlDF.show()
      // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
      val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))
      //     parsedDF.printSchema()

      // 4. Seleciona os campos desejados
      val selectedDF = parsedDF.select(
        $"NSUSVD",
        date_format(to_timestamp($"DHPROC", "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH").as("DHPROC_FORMATADO"),
        $"DHEMI",
        $"IP_TRANSMISSOR",
        $"MODELO",
        $"TPEMIS",
        $"parsed.protCTe.infProt._Id".as("infprot_id"),
        $"parsed.protCTe.infProt.cStat".as("protcte_cstat"),
        $"parsed.protCTe.infProt.chCTe".as("chave"),
        $"parsed.protCTe.infProt.dhRecbto".as("infprot_dhrecbto"),
        $"parsed.protCTe.infProt.digVal".as("infprot_digval"),
        $"parsed.protCTe.infProt.nProt".as("infprot_nprot"),
        $"parsed.protCTe.infProt.tpAmb".as("infprot_tpamb"),
        $"parsed.protCTe.infProt.xMotivo".as("infprot_xmotivo"),
        $"parsed.protCTe.infProt.verAplic".as("infprot_veraplic"),
        $"parsed.CTeOS._versao".as("versaoCTeOS"),
        $"parsed.CTeOS.infCTeSupl.qrCodCTe".as("qrCodCTe"),
        $"parsed.CTeOS.infCte._Id".as("idCTe"),
        $"parsed.CTeOS.infCte._versao".as("versaoInfCte"),
        $"parsed.CTeOS.infCte.autXML".as("autXML"),
        $"parsed.CTeOS.infCte.compl.ObsCont".as("compl_obsCont"),
        $"parsed.CTeOS.infCte.compl.ObsFisco".as("compl_obsFisco"),
        $"parsed.CTeOS.infCte.compl.xCaracAd".as("compl_xCaracAd"),
        $"parsed.CTeOS.infCte.compl.xCaracSer".as("compl_xCaracSer"),
        $"parsed.CTeOS.infCte.compl.xEmi".as("compl_xEmi"),
        $"parsed.CTeOS.infCte.compl.xObs".as("compl_xObs"),
        $"parsed.CTeOS.infCte.emit.CNPJ".as("emit__IE"),
        $"parsed.CTeOS.infCte.emit.enderEmit.CEP".as("enderEmit_CEP"),
        $"parsed.CTeOS.infCte.emit.enderEmit.UF".as("enderEmit_UF"),
        $"parsed.CTeOS.infCte.emit.enderEmit.cMun".as("enderEmit_CMun"),
        $"parsed.CTeOS.infCte.emit.enderEmit.fone".as("enderEmit_Fone"),
        $"parsed.CTeOS.infCte.emit.enderEmit.nro".as("enderEmit_Nro"),
        $"parsed.CTeOS.infCte.emit.enderEmit.xBairro".as("enderEmit_XBairro"),
        $"parsed.CTeOS.infCte.emit.enderEmit.xCpl".as("enderEmit_XCpl"),
        $"parsed.CTeOS.infCte.emit.enderEmit.xLgr".as("enderEmit_XLgr"),
        $"parsed.CTeOS.infCte.emit.enderEmit.xMun".as("enderEmit_XMun"),
        $"parsed.CTeOS.infCte.emit.xFant".as("enderEmit_XFant"),
        $"parsed.CTeOS.infCte.emit.xNome".as("enderEmit_XNome"),
        $"parsed.CTeOS.infCte.ide.CFOP".as("ide_CFOP"),
        $"parsed.CTeOS.infCte.ide.UFEnv".as("ide_UFEnv"),
        $"parsed.CTeOS.infCte.ide.UFFim".as("ide_UFFim"),
        $"parsed.CTeOS.infCte.ide.UFIni".as("ide_UFIni"),
        $"parsed.CTeOS.infCte.ide.cCT".as("ide_CCT"),
        $"parsed.CTeOS.infCte.ide.cDV".as("ide_CDV"),
        $"parsed.CTeOS.infCte.ide.cMunEnv".as("ide_CMunEnv"),
        $"parsed.CTeOS.infCte.ide.cMunFim".as("ide_CMunFim"),
        $"parsed.CTeOS.infCte.ide.cMunIni".as("ide_CMunIni"),
        $"parsed.CTeOS.infCte.ide.cUF".as("ide_CUF"),
        $"parsed.CTeOS.infCte.ide.dhEmi".as("ide_DhEmi"),
        $"parsed.CTeOS.infCte.ide.indIEToma".as("ide_IndIEToma"),
        $"parsed.CTeOS.infCte.ide.infPercurso".as("ide_InfPercurso"),
        $"parsed.CTeOS.infCte.ide.mod".as("ide_Mod"),
        $"parsed.CTeOS.infCte.ide.modal".as("ide_Modal"),
        $"parsed.CTeOS.infCte.ide.nCT".as("ide_NCT"),
        $"parsed.CTeOS.infCte.ide.natOp".as("ide_NatOp"),
        $"parsed.CTeOS.infCte.ide.procEmi".as("ide_ProcEmi"),
        $"parsed.CTeOS.infCte.ide.serie".as("ide_Serie"),
        $"parsed.CTeOS.infCte.ide.tpAmb".as("ide_TpAmb"),
        $"parsed.CTeOS.infCte.ide.tpCTe".as("ide_TpCTe"),
        $"parsed.CTeOS.infCte.ide.tpEmis".as("ide_TpEmis"),
        $"parsed.CTeOS.infCte.ide.tpImp".as("ide_TpImp"),
        $"parsed.CTeOS.infCte.ide.tpServ".as("ide_TpServ"),
        $"parsed.CTeOS.infCte.ide.verProc".as("ide_VerProc"),
        $"parsed.CTeOS.infCte.ide.xMunEnv".as("ide_XMunEnv"),
        $"parsed.CTeOS.infCte.ide.xMunFim".as("ide_XMunFim"),
        $"parsed.CTeOS.infCte.ide.xMunIni".as("ide_XMunIni"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.CST".as("icms_00CST"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.pICMS".as("icms_00PICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.vBC".as("icms_00VBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.vICMS".as("icms_00VICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.CST".as("icms_20CST"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.cBenef".as("icms_20CBenef"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.pICMS".as("icms_20PICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.pRedBC".as("icms_20PRedBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.vBC".as("icms_20VBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.vICMS".as("icms_20VICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.vICMSDeson".as("icms_20VICMSDeson"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS45.CST".as("icms_45CST"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS45.cBenef".as("icms_45CBenef"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS45.vICMSDeson".as("icms_45VICMSDeson"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.CST".as("icms_90CST"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.cBenef".as("icms_90CBenef"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.pICMS".as("icms_90PICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.pRedBC".as("icms_90PRedBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.vBC".as("icms_90VBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.vCred".as("icms_90VCred"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.vICMS".as("icms_90VICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.vICMSDeson".as("icms_90VICMSDeson"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSOutraUF.CST".as("icms_OutraUFCST"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSOutraUF.pICMSOutraUF".as("icms_OutraUFPICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSOutraUF.pRedBCOutraUF".as("icms_OutraUFPRedBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSOutraUF.vBCOutraUF".as("icms_OutraUFVBC"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSOutraUF.vICMSOutraUF".as("icms_OutraUFVICMS"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSSN.CST".as("icms_SNCST"),
        $"parsed.CTeOS.infCte.imp.ICMS.ICMSSN.indSN".as("icms_SNIndSN"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.pFCPUFFim".as("icms_UFFimPFCPUF"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.pICMSInter".as("icms_UFFimPICMSInter"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.pICMSUFFim".as("icms_UFFimPICMSUF"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.vBCUFFim".as("icms_UFFimVBCUF"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.vFCPUFFim".as("icms_UFFimVFCPUF"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.vICMSUFFim".as("icms_UFFimVICMSUF"),
        $"parsed.CTeOS.infCte.imp.ICMSUFFim.vICMSUFIni".as("icms_UFFimVICMSUFIni"),
        $"parsed.CTeOS.infCte.imp.infAdFisco".as("infAdFisco"),
        $"parsed.CTeOS.infCte.imp.infTribFed.vCOFINS".as("infTrib_FedVCOFINS"),
        $"parsed.CTeOS.infCte.imp.infTribFed.vCSLL".as("infTrib_FedVCSLL"),
        $"parsed.CTeOS.infCte.imp.infTribFed.vINSS".as("infTrib_FedVINSS"),
        $"parsed.CTeOS.infCte.imp.infTribFed.vIR".as("infTrib_FedVIR"),
        $"parsed.CTeOS.infCte.imp.infTribFed.vPIS".as("infTrib_FedVPIS"),
        $"parsed.CTeOS.infCte.imp.vTotTrib".as("vTotTrib"),
        $"parsed.CTeOS.infCte.infCTeNorm.cobr.dup".as("cobrDup"),
        $"parsed.CTeOS.infCte.infCTeNorm.cobr.fat.nFat".as("Fat_NFat"),
        $"parsed.CTeOS.infCte.infCTeNorm.cobr.fat.vDesc".as("Fat_VDesc"),
        $"parsed.CTeOS.infCte.infCTeNorm.cobr.fat.vLiq".as("Fat_VLiq"),
        $"parsed.CTeOS.infCte.infCTeNorm.cobr.fat.vOrig".as("Fat_VOrig"),
        $"parsed.CTeOS.infCte.infCTeNorm.infCteSub.chCte".as("infCteSub_ChCte"),
        $"parsed.CTeOS.infCte.infCTeNorm.infDocRef".as("infDocRef"),
        $"parsed.CTeOS.infCte.infCTeNorm.infGTVe".as("infGTVe"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal._versaoModal".as("infModalVersao"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.NroRegEstadual".as("rodoOS_NroRegEstadual"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.TAF".as("rodoOS_TAF"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.infFretamento.dhViagem".as("infFretamento_DhViagem"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.infFretamento.tpFretamento".as("infFretamento_TpFretamento"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.RENAVAM".as("Veic_RENAVAM"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.UF".as("Veic_UF"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.placa".as("Veic_Placa"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.CNPJ".as("Veic_Prop_CNPJ"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.CPF".as("Veic_Prop_CPF"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.IE".as("Veic_Prop_IE"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.NroRegEstadual".as("Veic_Prop_NroRegEstadual"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.TAF".as("Veic_Prop_TAF"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.UF".as("Veic_Prop_UF"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.tpProp".as("Veic_Prop_TpProp"),
        $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.xNome".as("Veic_Prop_XNome"),
        $"parsed.CTeOS.infCte.infCTeNorm.infServico.infQ.qCarga".as("infQ_oQCarga"),
        $"parsed.CTeOS.infCte.infCTeNorm.infServico.xDescServ".as("infServico_XDescServ"),
        $"parsed.CTeOS.infCte.infCTeNorm.seg".as("seg"),
        $"parsed.CTeOS.infCte.infCteComp.chCTe".as("infCteComp_ChCTe"),
        $"parsed.CTeOS.infCte.infRespTec.CNPJ".as("infRespTec_CNPJ"),
        $"parsed.CTeOS.infCte.infRespTec.email".as("infRespTec_Email"),
        $"parsed.CTeOS.infCte.infRespTec.fone".as("infRespTec_Fone"),
        $"parsed.CTeOS.infCte.infRespTec.hashCSRT".as("infRespTec_HashCSRT"),
        $"parsed.CTeOS.infCte.infRespTec.idCSRT".as("infRespTec_IdCSRT"),
        $"parsed.CTeOS.infCte.infRespTec.xContato".as("infRespTec_XContato"),
        $"parsed.CTeOS.infCte.toma.CNPJ".as("toma_CNPJ"),
        $"parsed.CTeOS.infCte.toma.CPF".as("toma_CPF"),
        $"parsed.CTeOS.infCte.toma.IE".as("toma_IE"),
        $"parsed.CTeOS.infCte.toma.email".as("toma_Email"),
        $"parsed.CTeOS.infCte.toma.enderToma.CEP".as("toma_CEP"),
        $"parsed.CTeOS.infCte.toma.enderToma.UF".as("toma_UF"),
        $"parsed.CTeOS.infCte.toma.enderToma.cMun".as("toma_CMun"),
        $"parsed.CTeOS.infCte.toma.enderToma.cPais".as("toma_CPais"),
        $"parsed.CTeOS.infCte.toma.enderToma.nro".as("toma_Nro"),
        $"parsed.CTeOS.infCte.toma.enderToma.xBairro".as("toma_XBairro"),
        $"parsed.CTeOS.infCte.toma.enderToma.xCpl".as("toma_XCpl"),
        $"parsed.CTeOS.infCte.toma.enderToma.xLgr".as("toma_XLgr"),
        $"parsed.CTeOS.infCte.toma.enderToma.xMun".as("toma_XMun"),
        $"parsed.CTeOS.infCte.toma.enderToma.xPais".as("toma_XPais"),
        $"parsed.CTeOS.infCte.toma.fone".as("toma_Fone"),
        $"parsed.CTeOS.infCte.toma.xFant".as("toma_XFant"),
        $"parsed.CTeOS.infCte.toma.xNome".as("toma_XNome"),
        $"parsed.CTeOS.infCte.vPrest.Comp".as("vPrest_Comp"),
        $"parsed.CTeOS.infCte.vPrest.vRec".as("vPrest_VRec"),
        $"parsed.CTeOS.infCte.vPrest.vTPrest".as("vPrest_VTPrest")
      )

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
      val repartitionedDF = selectedDFComParticao.repartition(2)

      // Escrever os dados particionados
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
        .partitionBy("chave_particao") // Garante a separação por partição
        .save("/datalake/prata/sources/dbms/dec/cte/CTeOS2")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")    }
  }
}

//CTeOSLegadoMensalProcessor.main(Array())