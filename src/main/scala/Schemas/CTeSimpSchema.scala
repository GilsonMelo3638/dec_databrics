package Schemas
import org.apache.spark.sql.types._

object CTeSimpSchema {
  def createSchema(): StructType = {
    new StructType()
      .add("CTeSimp", new StructType()
        .add("infCTeSupl", new StructType()
          .add("qrCodCTe", StringType, true))
        .add("infCte", new StructType()
          .add("_Id", StringType, true)
          .add("_versao", StringType, true)
          .add("compl", new StructType()
            .add("ObsCont", ArrayType(new StructType()
              .add("_xCampo", StringType, true)
              .add("xTexto", StringType, true)), true)
            .add("fluxo", new StructType()
              .add("xDest", StringType, true)
              .add("xOrig", StringType, true)
              .add("xRota", StringType, true))
            .add("xCaracAd", StringType, true)
            .add("xObs", StringType, true))
          .add("det", ArrayType(new StructType()
            .add("_nItem", StringType, true)
            .add("cMunFim", StringType, true)
            .add("cMunIni", StringType, true)
            .add("infNFe", new StructType()
              .add("chNFe", StringType, true)
              .add("dPrev", StringType, true))
            .add("vPrest", DoubleType, true)
            .add("vRec", DoubleType, true)
            .add("xMunFim", StringType, true)
            .add("xMunIni", StringType, true)), true)
          .add("emit", new StructType()
            .add("CNPJ", StringType, true)
            .add("CRT", StringType, true)
            .add("IE", StringType, true)
            .add("enderEmit", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("fone", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true))
            .add("xFant", StringType, true)
            .add("xNome", StringType, true))
          .add("ide", new StructType()
            .add("CFOP", StringType, true)
            .add("UFEnv", StringType, true)
            .add("UFFim", StringType, true)
            .add("UFIni", StringType, true)
            .add("cCT", StringType, true)
            .add("cDV", StringType, true)
            .add("cMunEnv", StringType, true)
            .add("cUF", StringType, true)
            .add("dhEmi", StringType, true)
            .add("mod", StringType, true)
            .add("modal", StringType, true)
            .add("nCT", StringType, true)
            .add("natOp", StringType, true)
            .add("procEmi", StringType, true)
            .add("retira", StringType, true)
            .add("serie", StringType, true)
            .add("tpAmb", StringType, true)
            .add("tpCTe", StringType, true)
            .add("tpEmis", StringType, true)
            .add("tpImp", StringType, true)
            .add("tpServ", StringType, true)
            .add("verProc", StringType, true)
            .add("xMunEnv", StringType, true))
          .add("imp", new StructType()
            .add("ICMS", new StructType()
              .add("ICMS00", new StructType()
                .add("CST", StringType, true)
                .add("pICMS", DoubleType, true)
                .add("vBC", DoubleType, true)
                .add("vICMS", DoubleType, true)))
            .add("vTotTrib", DoubleType, true))
          .add("infCarga", new StructType()
            .add("infQ", ArrayType(new StructType()
              .add("cUnid", StringType, true)
              .add("qCarga", DoubleType, true)
              .add("tpMed", StringType, true)), true)
            .add("proPred", StringType, true)
            .add("vCarga", DoubleType, true)
            .add("vCargaAverb", DoubleType, true))
          .add("infModal", new StructType()
            .add("_versaoModal", StringType, true)
            .add("rodo", new StructType()
              .add("RNTRC", StringType, true)))
          .add("infRespTec", new StructType()
            .add("CNPJ", StringType, true)
            .add("email", StringType, true)
            .add("fone", StringType, true)
            .add("xContato", StringType, true))
          .add("toma", new StructType()
            .add("CNPJ", StringType, true)
            .add("IE", StringType, true)
            .add("enderToma", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("cPais", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true)
              .add("xPais", StringType, true))
            .add("fone", StringType, true)
            .add("indIEToma", StringType, true)
            .add("toma", StringType, true)
            .add("xNome", StringType, true))
          .add("total", new StructType()
            .add("vTPrest", DoubleType, true)
            .add("vTRec", DoubleType, true))))
      .add("_versao", StringType, true)
      .add("_xmlns", StringType, true)
      .add("protCTe", new StructType()
        .add("_versao", StringType, true)
        .add("infProt", new StructType()
          .add("cStat", StringType, true)
          .add("chCTe", StringType, true)
          .add("dhRecbto", StringType, true)
          .add("digVal", StringType, true)
          .add("nProt", StringType, true)
          .add("tpAmb", StringType, true)
          .add("verAplic", StringType, true)
          .add("xMotivo", StringType, true)))
  }
}