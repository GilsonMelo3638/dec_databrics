package Schemas

import org.apache.spark.sql.types.{StructType, _}

object CTeOSSchema {
  def createSchema(): StructType = {
    new StructType()
      .add("CTeOS", new StructType()
        .add("_versao", StringType, true)
        .add("infCTeSupl", new StructType()
          .add("qrCodCTe", StringType, true))
        .add("infCte", new StructType()
          .add("_Id", StringType, true)
          .add("_versao", StringType, true)
          .add("autXML", ArrayType(new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true)), true)
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
              .add("xCpl", StringType, true)
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
            .add("cMunFim", StringType, true)
            .add("cMunIni", StringType, true)
            .add("cUF", StringType, true)
            .add("dhEmi", StringType, true)
            .add("indIEToma", StringType, true)
            .add("infPercurso", ArrayType(new StructType()
              .add("UFPer", StringType, true)), true)
            .add("mod", StringType, true)
            .add("modal", StringType, true)
            .add("nCT", StringType, true)
            .add("natOp", StringType, true)
            .add("procEmi", StringType, true)
            .add("serie", StringType, true)
            .add("tpAmb", StringType, true)
            .add("tpCTe", StringType, true)
            .add("tpEmis", StringType, true)
            .add("tpImp", StringType, true)
            .add("tpServ", StringType, true)
            .add("verProc", StringType, true)
            .add("xMunEnv", StringType, true)
            .add("xMunFim", StringType, true)
            .add("xMunIni", StringType, true))
          .add("imp", new StructType()
            .add("ICMS", new StructType()
              .add("ICMS00", new StructType()
                .add("CST", StringType, true)
                .add("pICMS", DoubleType, true)
                .add("vBC", DoubleType, true)
                .add("vICMS", DoubleType, true))
              .add("ICMS20", new StructType()
                .add("CST", StringType, true)
                .add("cBenef", StringType, true)
                .add("pICMS", DoubleType, true)
                .add("pRedBC", DoubleType, true)
                .add("vBC", DoubleType, true)
                .add("vICMS", DoubleType, true)
                .add("vICMSDeson", DoubleType, true))
              .add("ICMS45", new StructType()
                .add("CST", StringType, true)
                .add("cBenef", StringType, true)
                .add("vICMSDeson", StringType, true))
              .add("ICMS90", new StructType()
                .add("CST", StringType, true)
                .add("cBenef", StringType, true)
                .add("pICMS", DoubleType, true)
                .add("pRedBC", DoubleType, true)
                .add("vBC", DoubleType, true)
                .add("vCred", DoubleType, true)
                .add("vICMS", DoubleType, true)
                .add("vICMSDeson", DoubleType, true))
              .add("ICMSOutraUF", new StructType()
                .add("CST", StringType, true)
                .add("pICMSOutraUF", DoubleType, true)
                .add("pRedBCOutraUF", DoubleType, true)
                .add("vBCOutraUF", DoubleType, true)
                .add("vICMSOutraUF", DoubleType, true))
              .add("ICMSSN", new StructType()
                .add("CST", StringType, true)
                .add("indSN", StringType, true)))
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
                .add("dVenc", StringType, true)
                .add("nDup", StringType, true)
                .add("vDup", DoubleType, true)), true)
              .add("fat", new StructType()
                .add("nFat", StringType, true)
                .add("vDesc", DoubleType, true)
                .add("vLiq", DoubleType, true)
                .add("vOrig", DoubleType, true)))
            .add("infCteSub", new StructType()
              .add("chCte", StringType, true))
            .add("infDocRef", ArrayType(new StructType()
              .add("chBPe", StringType, true)
              .add("dEmi", StringType, true)
              .add("nDoc", StringType, true)
              .add("serie", StringType, true)
              .add("subserie", StringType, true)
              .add("vDoc", DoubleType, true)), true)
            .add("infGTVe", ArrayType(new StructType()
              .add("Comp", ArrayType(new StructType()
                .add("tpComp", StringType, true)
                .add("vComp", DoubleType, true)
                .add("xComp", StringType, true)), true)
              .add("chCTe", StringType, true)), true)
            .add("infModal", new StructType()
              .add("_versaoModal", StringType, true)
              .add("rodoOS", new StructType()
                .add("NroRegEstadual", StringType, true)
                .add("TAF", StringType, true)
                .add("infFretamento", new StructType()
                  .add("dhViagem", StringType, true)
                  .add("tpFretamento", StringType, true))
                .add("veic", new StructType()
                  .add("RENAVAM", StringType, true)
                  .add("UF", StringType, true)
                  .add("placa", StringType, true)
                  .add("prop", new StructType()
                    .add("CNPJ", StringType, true)
                    .add("CPF", StringType, true)
                    .add("IE", StringType, true)
                    .add("NroRegEstadual", StringType, true)
                    .add("TAF", StringType, true)
                    .add("UF", StringType, true)
                    .add("tpProp", StringType, true)
                    .add("xNome", StringType, true)))))
            .add("infServico", new StructType()
              .add("infQ", new StructType()
                .add("qCarga", DoubleType, true))
              .add("xDescServ", StringType, true))
            .add("seg", ArrayType(new StructType()
              .add("nApol", StringType, true)
              .add("respSeg", StringType, true)
              .add("xSeg", StringType, true)), true))
          .add("infCteComp", new StructType()
            .add("chCTe", StringType, true))
          .add("infRespTec", new StructType()
            .add("CNPJ", StringType, true)
            .add("email", StringType, true)
            .add("fone", StringType, true)
            .add("hashCSRT", StringType, true)
            .add("idCSRT", StringType, true)
            .add("xContato", StringType, true))
          .add("toma", new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true)
            .add("IE", StringType, true)
            .add("email", StringType, true)
            .add("enderToma", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("cPais", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true)
              .add("xPais", StringType, true))
            .add("fone", StringType, true)
            .add("xFant", StringType, true)
            .add("xNome", StringType, true))
          .add("vPrest", new StructType()
            .add("Comp", ArrayType(new StructType()
              .add("vComp", DoubleType, true)
              .add("xNome", StringType, true)), true)
            .add("vRec", DoubleType, true)
            .add("vTPrest", DoubleType, true))))
      .add("_dhConexao", StringType, true)
      .add("_ipTransmissor", StringType, true)
      .add("_nPortaCon", StringType, true)
      .add("_versao", StringType, true)
      .add("_xmlns", StringType, true)
      .add("protCTe", new StructType()
        .add("_versao", StringType, true)
        .add("infProt", new StructType()
          .add("_Id", StringType, true)
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