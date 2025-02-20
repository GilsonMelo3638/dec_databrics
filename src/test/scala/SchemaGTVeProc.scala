import com.databricks.spark.xml._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

val schema = new StructType()
  .add("GTVe", new StructType()
    .add("_versao", DoubleType, true)
    .add("infCTeSupl", new StructType()
      .add("qrCodCTe", StringType, true))
    .add("infCte", new StructType()
      .add("_Id", StringType, true)
      .add("_versao", DoubleType, true)
      .add("compl", new StructType()
        .add("xEmi", StringType, true)
        .add("xObs", StringType, true))
      .add("dest", new StructType()
        .add("CNPJ", LongType, true)
        .add("IE", StringType, true)
        .add("enderDest", new StructType()
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
        .add("xNome", StringType, true))
      .add("destino", new StructType()
        .add("CEP", LongType, true)
        .add("UF", StringType, true)
        .add("cMun", LongType, true)
        .add("nro", StringType, true)
        .add("xBairro", StringType, true)
        .add("xCpl", StringType, true)
        .add("xLgr", StringType, true)
        .add("xMun", StringType, true))
      .add("detGTV", new StructType()
        .add("infEspecie", ArrayType(new StructType()
          .add("tpEspecie", LongType, true)
          .add("tpNumerario", LongType, true)
          .add("vEspecie", DoubleType, true)
          .add("xMoedaEstr", StringType, true)), true)
        .add("infVeiculo", new StructType()
          .add("RNTRC", LongType, true)
          .add("UF", StringType, true)
          .add("placa", StringType, true))
        .add("qCarga", DoubleType, true))
      .add("emit", new StructType()
        .add("CNPJ", LongType, true)
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
        .add("cCT", LongType, true)
        .add("cDV", LongType, true)
        .add("cMunEnv", LongType, true)
        .add("cUF", LongType, true)
        .add("dhChegadaDest", TimestampType, true)
        .add("dhEmi", TimestampType, true)
        .add("dhSaidaOrig", TimestampType, true)
        .add("indIEToma", LongType, true)
        .add("mod", LongType, true)
        .add("modal", LongType, true)
        .add("nCT", LongType, true)
        .add("natOp", StringType, true)
        .add("serie", LongType, true)
        .add("toma", new StructType()
          .add("toma", LongType, true))
        .add("tomaTerceiro", new StructType()
          .add("CNPJ", LongType, true)
          .add("IE", StringType, true)
          .add("enderToma", new StructType()
            .add("CEP", LongType, true)
            .add("UF", StringType, true)
            .add("cMun", LongType, true)
            .add("nro", StringType, true)
            .add("xBairro", StringType, true)
            .add("xCpl", StringType, true)
            .add("xLgr", StringType, true)
            .add("xMun", StringType, true))
          .add("toma", LongType, true)
          .add("xNome", StringType, true))
        .add("tpAmb", LongType, true)
        .add("tpCTe", LongType, true)
        .add("tpEmis", LongType, true)
        .add("tpImp", LongType, true)
        .add("tpServ", LongType, true)
        .add("verProc", StringType, true)
        .add("xMunEnv", StringType, true))
      .add("infRespTec", new StructType()
        .add("CNPJ", LongType, true)
        .add("email", StringType, true)
        .add("fone", LongType, true)
        .add("xContato", StringType, true))
      .add("origem", new StructType()
        .add("CEP", LongType, true)
        .add("UF", StringType, true)
        .add("cMun", LongType, true)
        .add("nro", StringType, true)
        .add("xBairro", StringType, true)
        .add("xCpl", StringType, true)
        .add("xLgr", StringType, true)
        .add("xMun", StringType, true))
      .add("rem", new StructType()
        .add("CNPJ", LongType, true)
        .add("IE", StringType, true)
        .add("email", StringType, true)
        .add("enderReme", new StructType()
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
        .add("xNome", StringType, true))))
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
    val filteredDF: DataFrame = parquetDF.filter($"modelo" === 64)

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
//      $"GTVe._versao".as("gtve_versao"),
      $"GTVe.infCTeSupl.qrCodCTe".as("gtve_infctesupl_qrcodcte"),
      $"GTVe.infCte._Id".as("gtve_infcte_id"),
      $"GTVe.infCte._versao".as("gtve_infcte_versao"),
      $"GTVe.infCte.compl.xEmi".as("gtve_infcte_compl_xemi"),
      $"GTVe.infCte.compl.xObs".as("gtve_infcte_compl_xobs"),
      $"GTVe.infCte.dest.CNPJ".as("gtve_infcte_dest_cnpj"),
      $"GTVe.infCte.dest.IE".as("gtve_infcte_dest_ie"),
      $"GTVe.infCte.dest.xNome".as("gtve_infcte_dest_xnome"),
      $"GTVe.infCte.dest.enderDest.CEP".as("gtve_infcte_dest_enderdest_cep"),
      $"GTVe.infCte.dest.enderDest.UF".as("gtve_infcte_dest_enderdest_uf"),
      $"GTVe.infCte.dest.enderDest.cMun".as("gtve_infcte_dest_enderdest_cmun"),
      $"GTVe.infCte.dest.enderDest.cPais".as("gtve_infcte_dest_enderdest_cpais"),
      $"GTVe.infCte.dest.enderDest.nro".as("gtve_infcte_dest_enderdest_nro"),
      $"GTVe.infCte.dest.enderDest.xBairro".as("gtve_infcte_dest_enderdest_xbairro"),
      $"GTVe.infCte.dest.enderDest.xCpl".as("gtve_infcte_dest_enderdest_xcpl"),
      $"GTVe.infCte.dest.enderDest.xLgr".as("gtve_infcte_dest_enderdest_xlgr"),
      $"GTVe.infCte.dest.enderDest.xMun".as("gtve_infcte_dest_enderdest_xmun"),
      $"GTVe.infCte.dest.enderDest.xPais".as("gtve_infcte_dest_enderdest_xpais"),
      $"GTVe.infCte.emit.CNPJ".as("gtve_infcte_emit_cnpj"),
      $"GTVe.infCte.emit.IE".as("gtve_infcte_emit_ie"),
      $"GTVe.infCte.emit.xFant".as("gtve_infcte_emit_xfant"),
      $"GTVe.infCte.emit.xNome".as("gtve_infcte_emit_xnome"),
      $"GTVe.infCte.emit.enderEmit.CEP".as("gtve_infcte_emit_enderemit_cep"),
      $"GTVe.infCte.emit.enderEmit.UF".as("gtve_infcte_emit_enderemit_uf"),
      $"GTVe.infCte.emit.enderEmit.cMun".as("gtve_infcte_emit_enderemit_cmun"),
      $"GTVe.infCte.emit.enderEmit.fone".as("gtve_infcte_emit_enderemit_fone"),
      $"GTVe.infCte.emit.enderEmit.nro".as("gtve_infcte_emit_enderemit_nro"),
      $"GTVe.infCte.emit.enderEmit.xBairro".as("gtve_infcte_emit_enderemit_xbairro"),
      $"GTVe.infCte.emit.enderEmit.xCpl".as("gtve_infcte_emit_enderemit_xcpl"),
      $"GTVe.infCte.emit.enderEmit.xLgr".as("gtve_infcte_emit_enderemit_xlgr"),
      $"GTVe.infCte.emit.enderEmit.xMun".as("gtve_infcte_emit_enderemit_xmun"),
      $"GTVe.infCte.ide.CFOP".as("gtve_infcte_ide_cfop"),
      $"GTVe.infCte.ide.UFEnv".as("gtve_infcte_ide_ufenv"),
      $"GTVe.infCte.ide.cMunEnv".as("gtve_infcte_ide_cmunenv"),
      $"GTVe.infCte.ide.dhEmi".as("gtve_infcte_ide_dhemi"),
      $"GTVe.infCte.ide.nCT".as("gtve_infcte_ide_nct"),
      $"GTVe.infCte.ide.natOp".as("gtve_infcte_ide_natop"),
      $"GTVe.infCte.ide.serie".as("gtve_infcte_ide_serie"),
      $"GTVe.infCte.ide.tpAmb".as("gtve_infcte_ide_tpamb"),
      $"GTVe.infCte.ide.tpCTe".as("gtve_infcte_ide_tpcte"),
      $"GTVe.infCte.ide.tpEmis".as("gtve_infcte_ide_tpemis"),
      $"GTVe.infCte.ide.verProc".as("gtve_infcte_ide_verproc"),
      $"_dhConexao".as("dhconexao"),
      $"_ipTransmissor".as("iptransmissor"),
      $"_nPortaCon".as("nportacon"),
      $"_versao".as("versao"),
      $"_xmlns".as("xmlns"),
      $"protCTe._versao".as("protcte_versao"),
      $"protCTe.infProt._Id".as("protcte_infprot_id"),
      $"protCTe.infProt.cStat".as("protcte_infprot_cstat"),
      $"protCTe.infProt.chCTe".as("chave"),
      $"protCTe.infProt.dhRecbto".as("protcte_infprot_dhrecbto"),
      $"protCTe.infProt.digVal".as("protcte_infprot_digval"),
      $"protCTe.infProt.nProt".as("protcte_infprot_nprot"),
      $"protCTe.infProt.tpAmb".as("protcte_infprot_tpamb"),
      $"protCTe.infProt.verAplic".as("protcte_infprot_veraplic")
    )
    dfSelect.show(10, truncate = false)
    //  df.show(2, truncate = false)
    xmlDF.printSchema()
  }
}
ExtractCTeOS.main(Array())