import com.databricks.spark.xml._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

val schema = new StructType()
  .add("CTeOS", new StructType()
    .add("_versao", DoubleType, true)
    .add("infCTeSupl", new StructType()
      .add("qrCodCTe", StringType, true))
    .add("infCte", new StructType()
      .add("_Id", StringType, true)
      .add("_versao", DoubleType, true)
      .add("autXML", ArrayType(new StructType()
        .add("CNPJ", LongType, true)
        .add("CPF", LongType, true)), true)
      .add("compl", new StructType()
        .add("ObsCont", ArrayType(new StructType()
          .add("_xCampo", StringType, true)
          .add("xTexto", StringType, true)), true)
        .add("ObsFisco", ArrayType(new StructType()
          .add("_xCampo", StringType, true)
          .add("xTexto", StringType, true)), true)
        .add("xCaracAd", StringType, true)
        .add("xCaracSer", StringType, true)
        .add("xEmi", StringType, true)
        .add("xObs", StringType, true))
      .add("emit", new StructType()
        .add("CNPJ", LongType, true)
        .add("CRT", LongType, true)
        .add("IE", LongType, true)
        .add("enderEmit", new StructType()
          .add("CEP", LongType, true)
          .add("UF", StringType, true)
          .add("cMun", LongType, true)
          .add("fone", LongType, true)
          .add("nro", StringType, true)
          .add("xBairro", StringType, true)
          .add("xCpl", StringType, true)
          .add("xLgr", StringType, true)
          .add("xMun", StringType, true))
        .add("xFant", StringType, true)
        .add("xNome", StringType, true))
      .add("ide", new StructType()
        .add("CFOP", LongType, true)
        .add("UFEnv", StringType, true)
        .add("UFFim", StringType, true)
        .add("UFIni", StringType, true)
        .add("cCT", LongType, true)
        .add("cDV", LongType, true)
        .add("cMunEnv", LongType, true)
        .add("cMunFim", LongType, true)
        .add("cMunIni", LongType, true)
        .add("cUF", LongType, true)
        .add("dhEmi", TimestampType, true)
        .add("indIEToma", LongType, true)
        .add("infPercurso", ArrayType(new StructType()
          .add("UFPer", StringType, true)), true)
        .add("mod", LongType, true)
        .add("modal", LongType, true)
        .add("nCT", LongType, true)
        .add("natOp", StringType, true)
        .add("procEmi", LongType, true)
        .add("serie", LongType, true)
        .add("tpAmb", LongType, true)
        .add("tpCTe", LongType, true)
        .add("tpEmis", LongType, true)
        .add("tpImp", LongType, true)
        .add("tpServ", LongType, true)
        .add("verProc", StringType, true)
        .add("xMunEnv", StringType, true)
        .add("xMunFim", StringType, true)
        .add("xMunIni", StringType, true))
      .add("imp", new StructType()
        .add("ICMS", new StructType()
          .add("ICMS00", new StructType()
            .add("CST", LongType, true)
            .add("pICMS", DoubleType, true)
            .add("vBC", DoubleType, true)
            .add("vICMS", DoubleType, true))
          .add("ICMS20", new StructType()
            .add("CST", LongType, true)
            .add("cBenef", StringType, true)
            .add("pICMS", DoubleType, true)
            .add("pRedBC", DoubleType, true)
            .add("vBC", DoubleType, true)
            .add("vICMS", DoubleType, true)
            .add("vICMSDeson", DoubleType, true))
          .add("ICMS45", new StructType()
            .add("CST", LongType, true)
            .add("cBenef", LongType, true)
            .add("vICMSDeson", LongType, true))
          .add("ICMS90", new StructType()
            .add("CST", LongType, true)
            .add("cBenef", StringType, true)
            .add("pICMS", DoubleType, true)
            .add("pRedBC", DoubleType, true)
            .add("vBC", DoubleType, true)
            .add("vCred", DoubleType, true)
            .add("vICMS", DoubleType, true)
            .add("vICMSDeson", DoubleType, true))
          .add("ICMSOutraUF", new StructType()
            .add("CST", LongType, true)
            .add("pICMSOutraUF", DoubleType, true)
            .add("pRedBCOutraUF", DoubleType, true)
            .add("vBCOutraUF", DoubleType, true)
            .add("vICMSOutraUF", DoubleType, true))
          .add("ICMSSN", new StructType()
            .add("CST", LongType, true)
            .add("indSN", LongType, true)))
        .add("ICMSUFFim", new StructType()
          .add("pFCPUFFim", DoubleType, true)
          .add("pICMSInter", DoubleType, true)
          .add("pICMSUFFim", DoubleType, true)
          .add("vBCUFFim", DoubleType, true)
          .add("vFCPUFFim", DoubleType, true)
          .add("vICMSUFFim", DoubleType, true)
          .add("vICMSUFIni", DoubleType, true))
        .add("infAdFisco", StringType, true)
        .add("infTribFed", new StructType()
          .add("vCOFINS", DoubleType, true)
          .add("vCSLL", DoubleType, true)
          .add("vINSS", DoubleType, true)
          .add("vIR", DoubleType, true)
          .add("vPIS", DoubleType, true))
        .add("vTotTrib", DoubleType, true))
      .add("infCTeNorm", new StructType()
        .add("cobr", new StructType()
          .add("dup", ArrayType(new StructType()
            .add("dVenc", DateType, true)
            .add("nDup", StringType, true)
            .add("vDup", DoubleType, true)), true)
          .add("fat", new StructType()
            .add("nFat", StringType, true)
            .add("vDesc", DoubleType, true)
            .add("vLiq", DoubleType, true)
            .add("vOrig", DoubleType, true)))
        .add("infCteSub", new StructType()
          .add("chCte", DoubleType, true))
        .add("infDocRef", ArrayType(new StructType()
          .add("chBPe", DoubleType, true)
          .add("dEmi", DateType, true)
          .add("nDoc", LongType, true)
          .add("serie", LongType, true)
          .add("subserie", LongType, true)
          .add("vDoc", DoubleType, true)), true)
        .add("infGTVe", ArrayType(new StructType()
          .add("Comp", ArrayType(new StructType()
            .add("tpComp", LongType, true)
            .add("vComp", DoubleType, true)
            .add("xComp", StringType, true)), true)
          .add("chCTe", DoubleType, true)), true)
        .add("infModal", new StructType()
          .add("_versaoModal", DoubleType, true)
          .add("rodoOS", new StructType()
            .add("NroRegEstadual", DoubleType, true)
            .add("TAF", LongType, true)
            .add("infFretamento", new StructType()
              .add("dhViagem", TimestampType, true)
              .add("tpFretamento", LongType, true))
            .add("veic", new StructType()
              .add("RENAVAM", StringType, true)
              .add("UF", StringType, true)
              .add("placa", StringType, true)
              .add("prop", new StructType()
                .add("CNPJ", LongType, true)
                .add("CPF", LongType, true)
                .add("IE", StringType, true)
                .add("NroRegEstadual", LongType, true)
                .add("TAF", LongType, true)
                .add("UF", StringType, true)
                .add("tpProp", LongType, true)
                .add("xNome", StringType, true)))))
        .add("infServico", new StructType()
          .add("infQ", new StructType()
            .add("qCarga", DoubleType, true))
          .add("xDescServ", StringType, true))
        .add("seg", ArrayType(new StructType()
          .add("nApol", StringType, true)
          .add("respSeg", LongType, true)
          .add("xSeg", StringType, true)), true))
      .add("infCteComp", new StructType()
        .add("chCTe", DoubleType, true))
      .add("infRespTec", new StructType()
        .add("CNPJ", LongType, true)
        .add("email", StringType, true)
        .add("fone", LongType, true)
        .add("hashCSRT", StringType, true)
        .add("idCSRT", LongType, true)
        .add("xContato", StringType, true))
      .add("toma", new StructType()
        .add("CNPJ", LongType, true)
        .add("CPF", LongType, true)
        .add("IE", StringType, true)
        .add("email", StringType, true)
        .add("enderToma", new StructType()
          .add("CEP", LongType, true)
          .add("UF", StringType, true)
          .add("cMun", LongType, true)
          .add("cPais", LongType, true)
          .add("nro", StringType, true)
          .add("xBairro", StringType, true)
          .add("xCpl", StringType, true)
          .add("xLgr", StringType, true)
          .add("xMun", StringType, true)
          .add("xPais", StringType, true))
        .add("fone", LongType, true)
        .add("xFant", StringType, true)
        .add("xNome", StringType, true))
      .add("vPrest", new StructType()
        .add("Comp", ArrayType(new StructType()
          .add("vComp", DoubleType, true)
          .add("xNome", StringType, true)), true)
        .add("vRec", DoubleType, true)
        .add("vTPrest", DoubleType, true))))
  .add("_dhConexao", TimestampType, true)
  .add("_ipTransmissor", StringType, true)
  .add("_nPortaCon", LongType, true)
  .add("_versao", DoubleType, true)
  .add("_xmlns", StringType, true)
  .add("protCTe", new StructType()
    .add("_versao", DoubleType, true)
    .add("infProt", new StructType()
      .add("_Id", StringType, true)
      .add("cStat", LongType, true)
      .add("chCTe", DoubleType, true)
      .add("dhRecbto", TimestampType, true)
      .add("digVal", StringType, true)
      .add("nProt", LongType, true)
      .add("tpAmb", LongType, true)
      .add("verAplic", StringType, true)
      .add("xMotivo", StringType, true)))
object ExtractCTeOS {
  def main(args: Array[String]): Unit = {
    // Criação da sessão Spark com suporte ao Hive
    // Criar sessão Spark otimizada
    val spark = SparkSession.builder()
      .appName("ExtractInfNFe")
      .config("spark.sql.parquet.writeLegacyFormat", "true") // Evita problemas de compatibilidade
      .config("spark.executor.memory", "8g") // Aumenta memória do executor
      .config("spark.executor.cores", "2") // Limita núcleos por executor
      .config("spark.driver.memory", "4g") // Aumenta memória do driver
      .config("spark.network.timeout", "600s") // Timeout para evitar desconexões
      .config("spark.sql.shuffle.partitions", "200") // Otimiza o número de partições
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Carregar o DataFrame do Parquet
    val parquetDF: DataFrame = spark.read.parquet("/datalake/bronze/sources/dbms/dec/cte/2024")
    // Filtrar a coluna 'modelo' para incluir apenas registros onde modelo == 67
    val filteredDF: DataFrame = parquetDF.filter($"modelo" === 67)
    // Selecionar apenas a coluna 'XML_DOCUMENTO_CLOB' e converter para Dataset[String]
    val xmlDF = filteredDF.select($"XML_DOCUMENTO_CLOB".as[String]).cache()
    // Exibir os primeiros registros (XML completo)
    xmlDF.show(truncate = false)
    // Inferir o schema do XML
    val inferredSchema = spark.read
      .option("rowTag", "GTVeProc") // Tag raiz do XML
      .xml(xmlDF)
    // Exibir o schema inferido
    inferredSchema.printSchema()

    val dfSelect = inferredSchema.select(
      $"CTeOS.infCte.emit.CNPJ".as("emit_cnpj"),
      $"CTeOS.infCte.emit.CRT".as("emit_crt"),
      $"CTeOS.infCte.emit.IE".as("emit_ie"),
      $"CTeOS.infCte.emit.enderEmit.CEP".as("enderemit_cep"),
      $"CTeOS.infCte.emit.enderEmit.UF".as("enderemit_uf"),
      $"CTeOS.infCte.emit.enderEmit.cMun".as("enderemit_cmun"),
      $"CTeOS.infCte.emit.enderEmit.fone".as("enderemit_fone"),
      $"CTeOS.infCte.emit.enderEmit.nro".as("enderemit_nro"),
      $"CTeOS.infCte.emit.enderEmit.xBairro".as("enderemit_xbairro"),
      $"CTeOS.infCte.emit.enderEmit.xCpl".as("enderemit_xcpl"),
      $"CTeOS.infCte.emit.enderEmit.xLgr".as("enderemit_xlgr"),
      $"CTeOS.infCte.emit.enderEmit.xMun".as("enderemit_xmun"),
      $"CTeOS.infCte.emit.xFant".as("emit_xfant"),
      $"CTeOS.infCte.emit.xNome".as("emit_xnome"),
      $"CTeOS.infCte.ide.CFOP".as("ide_cfop"),
      $"CTeOS.infCte.ide.UFEnv".as("ide_ufenv"),
      $"CTeOS.infCte.ide.UFFim".as("ide_uffim"),
      $"CTeOS.infCte.ide.UFIni".as("ide_ufini"),
      $"CTeOS.infCte.ide.cCT".as("ide_cct"),
      $"CTeOS.infCte.ide.cDV".as("ide_cdv"),
      $"CTeOS.infCte.ide.cMunEnv".as("ide_cmunenv"),
      $"CTeOS.infCte.ide.cMunFim".as("ide_cmunfim"),
      $"CTeOS.infCte.ide.cMunIni".as("ide_cmunini"),
      $"CTeOS.infCte.ide.cUF".as("ide_cuf"),
      $"CTeOS.infCte.ide.dhEmi".as("ide_dhemi"),
      $"CTeOS.infCte.ide.indIEToma".as("ide_indietoma"),
      $"CTeOS.infCte.ide.mod".as("ide_mod"),
      $"CTeOS.infCte.ide.modal".as("ide_modal"),
      $"CTeOS.infCte.ide.nCT".as("ide_nct"),
      $"CTeOS.infCte.ide.natOp".as("ide_natop"),
      $"CTeOS.infCte.ide.procEmi".as("ide_procemi"),
      $"CTeOS.infCte.ide.serie".as("ide_serie"),
      $"CTeOS.infCte.ide.tpAmb".as("ide_tpamb"),
      $"CTeOS.infCte.ide.tpCTe".as("ide_tpcte"),
      $"CTeOS.infCte.ide.tpEmis".as("ide_tpemis"),
      $"CTeOS.infCte.ide.tpImp".as("ide_tpimp"),
      $"CTeOS.infCte.ide.tpServ".as("ide_tpserv"),
      $"CTeOS.infCte.ide.verProc".as("ide_verproc"),
      $"CTeOS.infCte.ide.xMunEnv".as("ide_xmunenv"),
      $"CTeOS.infCte.ide.xMunFim".as("ide_xmunfim"),
      $"CTeOS.infCte.ide.xMunIni".as("ide_xmunini"),
      $"CTeOS.infCte.imp.ICMS.ICMS00.CST".as("icms00_cst"),
      $"CTeOS.infCte.imp.ICMS.ICMS00.pICMS".as("icms00_picms"),
      $"CTeOS.infCte.imp.ICMS.ICMS00.vBC".as("icms00_vbc"),
      $"CTeOS.infCte.imp.ICMS.ICMS00.vICMS".as("icms00_vicms"),
      $"CTeOS.infCte.imp.ICMS.ICMS20.CST".as("icms20_cst"),
      $"CTeOS.infCte.imp.ICMS.ICMS20.pICMS".as("icms20_picms"),
      $"CTeOS.infCte.imp.ICMS.ICMS20.vBC".as("icms20_vbc"),
      $"CTeOS.infCte.imp.ICMS.ICMS20.vICMS".as("icms20_vicms"),
      $"CTeOS.infCte.imp.ICMS.ICMS45.CST".as("icms45_cst"),
      $"CTeOS.infCte.imp.ICMS.ICMS90.CST".as("icms90_cst"),
      $"CTeOS.infCte.imp.ICMS.ICMS90.pICMS".as("icms90_picms"),
      $"CTeOS.infCte.imp.ICMS.ICMS90.vBC".as("icms90_vbc"),
      $"CTeOS.infCte.imp.ICMS.ICMS90.vICMS".as("icms90_vicms"),
      $"CTeOS.infCte.imp.ICMSUFFim.vICMSUFFim".as("icmsuffim_vicmsuffim"),
      $"CTeOS.infCte.imp.vTotTrib".as("imp_vtottrib"),
      $"CTeOS.infCte.infCTeNorm.cobr.fat.nFat".as("fat_nfat"),
      $"CTeOS.infCte.infCTeNorm.cobr.fat.vLiq".as("fat_vliq"),
      $"CTeOS.infCte.infCTeNorm.infDocRef".as("infDocRef"),
      $"CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.placa".as("veic_placa"),
      $"CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.CNPJ".as("veic_prop_cnpj"),
      $"CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.xNome".as("veic_prop_xnome"),
      $"CTeOS.infCte.infCTeNorm.infServico.infQ.qCarga".as("infq_qcarga"),
      $"CTeOS.infCte.infCTeNorm.infServico.xDescServ".as("infserv_xdescserv"),
      $"CTeOS.infCte.infRespTec.CNPJ".as("infresptec_cnpj"),
      $"CTeOS.infCte.infRespTec.email".as("infresptec_email"),
      $"CTeOS.infCte.infRespTec.fone".as("infresptec_fone"),
      $"CTeOS.infCte.infRespTec.xContato".as("infresptec_xcontato"),
      $"CTeOS.infCte.toma.CNPJ".as("toma_cnpj"),
      $"CTeOS.infCte.toma.CPF".as("toma_cpf"),
      $"CTeOS.infCte.toma.IE".as("toma_ie"),
      $"CTeOS.infCte.toma.enderToma.CEP".as("toma_cep"),
      $"CTeOS.infCte.toma.enderToma.UF".as("toma_uf"),
      $"CTeOS.infCte.toma.enderToma.cMun".as("toma_cmun"),
      $"CTeOS.infCte.toma.enderToma.nro".as("toma_nro"),
      $"CTeOS.infCte.toma.enderToma.xBairro".as("toma_xbairro"),
      $"CTeOS.infCte.toma.enderToma.xLgr".as("toma_xlgr"),
      $"CTeOS.infCte.toma.enderToma.xMun".as("toma_xmun"),
      $"CTeOS.infCte.toma.xNome".as("toma_xnome"),
      $"CTeOS.infCte.vPrest.vTPrest".as("vprest_vtprest"),
      $"protCTe.infProt.cStat".as("protcte_cstat"),
      $"protCTe.infProt.dhRecbto".as("protcte_dhrecbto"),
      $"protCTe.infProt.nProt".as("protcte_nprot"),
      $"protCTe.infProt.xMotivo".as("protcte_xmotivo")
    )
    dfSelect.show(10, truncate = false)
    //  df.show(2, truncate = false)
    xmlDF.printSchema()
  }
}
ExtractCTeOS.main(Array())