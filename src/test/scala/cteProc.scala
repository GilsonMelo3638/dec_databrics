//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import com.databricks.spark.xml.functions.from_xml  // Import necessário
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types._
//
//val schema = new StructType()
//  .add("CTe", new StructType()
//    .add("infCte", new StructType()
//      .add("ide", new StructType()
//        .add("CFOP", LongType, true)
//        .add("UFEnv", StringType, true)
//        .add("UFFim", StringType, true)
//        .add("UFIni", StringType, true)
//        .add("cCT", LongType, true)
//        .add("cDV", LongType, true)
//        .add("cMunEnv", LongType, true)
//        .add("cMunFim", LongType, true)
//        .add("cMunIni", LongType, true)
//        .add("cUF", LongType, true)
//        .add("dhCont", TimestampType, true)
//        .add("dhEmi", TimestampType, true)
//        .add("indGlobalizado", LongType, true)
//        .add("indIEToma", LongType, true)
//        .add("mod", LongType, true)
//        .add("modal", LongType, true)
//        .add("nCT", LongType, true)
//        .add("natOp", StringType, true)
//        .add("procEmi", LongType, true)
//        .add("retira", LongType, true)
//        .add("serie", LongType, true)
//        .add("toma3", new StructType()
//          .add("toma", LongType, true)
//          , true)
//        .add("toma4", new StructType()
//          .add("CNPJ", LongType, true)
//          .add("CPF", LongType, true)
//          .add("IE", StringType, true)
//          .add("email", StringType, true)
//          .add("enderToma", new StructType()
//            .add("CEP", LongType, true)
//            .add("UF", StringType, true)
//            .add("cMun", LongType, true)
//            .add("cPais", LongType, true)
//            .add("nro", StringType, true)
//            .add("xBairro", StringType, true)
//            .add("xCpl", StringType, true)
//            .add("xLgr", StringType, true)
//            .add("xMun", StringType, true)
//            .add("xPais", StringType, true)
//            , true)
//          .add("fone", LongType, true)
//          .add("toma", LongType, true)
//          .add("xFant", StringType, true)
//          .add("xNome", StringType, true)
//          , true)
//        .add("tpAmb", LongType, true)
//        .add("tpCTe", LongType, true)
//        .add("tpEmis", LongType, true)
//        .add("tpImp", LongType, true)
//        .add("tpServ", LongType, true)
//        .add("verProc", StringType, true)
//        .add("xDetRetira", StringType, true)
//        .add("xJust", StringType, nullable = true)
//        .add("xMunEnv", StringType, true)
//        .add("xMunFim", StringType, true)
//        .add("xMunIni", StringType, true)
//        , true)
//      .add("emit", new StructType()
//        .add("CNPJ", LongType, true)
//        .add("CPF", LongType, true)
//        .add("CRT", LongType, true)
//        .add("IE", LongType, true)
//        .add("IEST", LongType, true)
//        .add("enderEmit", new StructType()
//          .add("CEP", LongType, true)
//          .add("UF", StringType, true)
//          .add("cMun", LongType, true)
//          .add("fone", LongType, true)
//          .add("nro", StringType, true)
//          .add("xBairro", StringType, true)
//          .add("xCpl", StringType, true)
//          .add("xLgr", StringType, true)
//          .add("xMun", StringType, true)
//          , true)
//        .add("xFant", StringType, true)
//        .add("xNome", StringType, true)
//        , true)
//      .add("dest", new StructType()
//        .add("CNPJ", LongType, true)
//        .add("CPF", LongType, true)
//        .add("IE", StringType, true)
//        .add("ISUF", LongType, true)
//        .add("email", StringType, true)
//        .add("enderDest", new StructType()
//          .add("CEP", LongType, true)
//          .add("UF", StringType, true)
//          .add("cMun", LongType, true)
//          .add("cPais", LongType, true)
//          .add("nro", StringType, true)
//          .add("xBairro", StringType, true)
//          .add("xCpl", StringType, true)
//          .add("xLgr", StringType, true)
//          .add("xMun", StringType, true)
//          .add("xPais", StringType, true)
//          , true)
//        .add("fone", LongType, true)
//        .add("xNome", StringType, true)
//        , true)
//      .add("rem", StringType, true)
//      .add("vPrest", StringType, true)
//      , true)
//  )
//object ExtractInfCTe {
//  def main(args: Array[String]): Unit = {
//    // Criação da sessão Spark com suporte ao Hive
//    val spark = SparkSession.builder.appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
//    import spark.implicits._
//    // Diretório dos arquivos Parquet
//    // Carregar o DataFrame a partir do diretório Parquet, assumindo que o XML completo está em 'XML_DOCUMENTO_CLOB'
//    val df = spark.read.format("xml").option("rowTag", "cteProc").load("/datalake/bronze/sources/dbms/dec/cte/2024")
//    import org.apache.spark.sql.functions._
//
//    val dfSelect = df.select(
//      col("CTe.infCte.ide.CFOP").alias("ide_cfop"),
//      col("CTe.infCte.ide.UFEnv").alias("ide_ufenv"),
//      col("CTe.infCte.ide.UFFim").alias("ide_uffim"),
//      col("CTe.infCte.ide.UFIni").alias("ide_ufini"),
//      col("CTe.infCte.ide.cCT").alias("ide_cct"),
//      col("CTe.infCte.ide.cDV").alias("ide_cdv"),
//      col("CTe.infCte.ide.cMunEnv").alias("ide_cmunenv"),
//      col("CTe.infCte.ide.cMunFim").alias("ide_cmunfim"),
//      col("CTe.infCte.ide.cMunIni").alias("ide_cmunini"),
//      col("CTe.infCte.ide.cUF").alias("ide_cuf"),
//      col("CTe.infCte.ide.dhCont").alias("ide_dhcont"),
//      col("CTe.infCte.ide.dhEmi").alias("ide_dhemi"),
//      col("CTe.infCte.ide.indGlobalizado").alias("ide_indglobalizado"),
//      col("CTe.infCte.ide.indIEToma").alias("ide_indietoma"),
//      col("CTe.infCte.ide.mod").alias("ide_mod"),
//      col("CTe.infCte.ide.modal").alias("ide_modal"),
//      col("CTe.infCte.ide.nCT").alias("ide_nct"),
//      col("CTe.infCte.ide.natOp").alias("ide_natop"),
//      col("CTe.infCte.ide.procEmi").alias("ide_procemi"),
//      col("CTe.infCte.ide.retira").alias("ide_retira"),
//      col("CTe.infCte.ide.serie").alias("ide_serie"),
//      col("CTe.infCte.ide.toma3.toma").alias("ide_toma3_toma"),
//      col("CTe.infCte.ide.toma4.CNPJ").alias("ide_toma4_cnpj"),
//      col("CTe.infCte.ide.toma4.CPF").alias("ide_toma4_cpf"),
//      col("CTe.infCte.ide.toma4.IE").alias("ide_toma4_ie"),
//      col("CTe.infCte.ide.toma4.email").alias("ide_toma4_email"),
//      col("CTe.infCte.ide.toma4.enderToma.CEP").alias("ide_toma4_endertoma_cep"),
//      col("CTe.infCte.ide.toma4.enderToma.UF").alias("ide_toma4_endertoma_uf"),
//      col("CTe.infCte.ide.toma4.enderToma.cMun").alias("ide_toma4_endertoma_cmun"),
//      col("CTe.infCte.ide.toma4.enderToma.cPais").alias("ide_toma4_endertoma_cpais"),
//      col("CTe.infCte.ide.toma4.enderToma.nro").alias("ide_toma4_endertoma_nro"),
//      col("CTe.infCte.ide.toma4.enderToma.xBairro").alias("ide_toma4_endertoma_xbairro"),
//      col("CTe.infCte.ide.toma4.enderToma.xCpl").alias("ide_toma4_endertoma_xcpl"),
//      col("CTe.infCte.ide.toma4.enderToma.xLgr").alias("ide_toma4_endertoma_xlgr"),
//      col("CTe.infCte.ide.toma4.enderToma.xMun").alias("ide_toma4_endertoma_xmun"),
//      col("CTe.infCte.ide.toma4.enderToma.xPais").alias("ide_toma4_endertoma_xpais"),
//      col("CTe.infCte.ide.toma4.fone").alias("ide_toma4_fone"),
//      col("CTe.infCte.ide.toma4.toma").alias("ide_toma4_toma"),
//      col("CTe.infCte.ide.toma4.xFant").alias("ide_toma4_xfant"),
//      col("CTe.infCte.ide.toma4.xNome").alias("ide_toma4_xnome"),
//      col("CTe.infCte.ide.tpAmb").alias("ide_tpamb"),
//      col("CTe.infCte.ide.tpCTe").alias("ide_tpcte"),
//      col("CTe.infCte.ide.tpEmis").alias("ide_tpemis"),
//      col("CTe.infCte.ide.tpImp").alias("ide_tpimp"),
//      col("CTe.infCte.ide.tpServ").alias("ide_tpserv"),
//      col("CTe.infCte.ide.verProc").alias("ide_verproc"),
//      col("CTe.infCte.ide.xDetRetira").alias("ide_xdetretira"),
//      col("CTe.infCte.ide.xJust").alias("ide_xjust"),
//      col("CTe.infCte.ide.xMunEnv").alias("ide_xmunenv"),
//      col("CTe.infCte.ide.xMunFim").alias("ide_xmunfim"),
//      col("CTe.infCte.ide.xMunIni").alias("ide_xmunini")
//   ).show(10, truncate = false)
//
//    //  df.show(2, truncate = false)
//    df.printSchema()
//  }
//}
////ExtractInfCTe.main(Array())