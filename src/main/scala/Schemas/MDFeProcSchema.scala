package Schemas

import org.apache.spark.sql.types.{DoubleType, StringType, StructType, _}

object MDFeProcSchema {
  def createSchema(): StructType = {
    new StructType()
      .add("MDFe", new StructType()
        .add("infMDFe", new StructType()
          .add("_Id", StringType, true)
          .add("_versao", StringType, true)
          .add("autXML", ArrayType(new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true), true))
          .add("emit", new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true)
            .add("IE", StringType, true)
            .add("enderEmit", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("email", StringType, true)
              .add("fone", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true))
            .add("xFant", StringType, true)
            .add("xNome", StringType, true))
          .add("ide", new StructType()
            .add("UFFim", StringType, true)
            .add("UFIni", StringType, true)
            .add("cDV", StringType, true)
            .add("cMDF", StringType, true)
            .add("cUF", StringType, true)
            .add("dhEmi", StringType, true)
            .add("dhIniViagem", StringType, true)
            .add("indCanalVerde", StringType, true)
            .add("infMunCarrega", ArrayType(new StructType()
              .add("cMunCarrega", StringType, true)
              .add("xMunCarrega", StringType, true)), true)
            .add("infPercurso", ArrayType(new StructType()
              .add("UFPer", StringType, true)), true)
            .add("mod", StringType, true)
            .add("modal", StringType, true)
            .add("nMDF", StringType, true)
            .add("procEmi", StringType, true)
            .add("serie", StringType, true)
            .add("tpAmb", StringType, true)
            .add("tpEmis", StringType, true)
            .add("tpEmit", StringType, true)
            .add("tpTransp", StringType, true)
            .add("verProc", StringType, true))
          .add("infAdic", new StructType()
            .add("infAdFisco", StringType, true)
            .add("infCpl", StringType, true))
          .add("infDoc", new StructType()
            .add("infMunDescarga", ArrayType(new StructType()
              .add("cMunDescarga", StringType, true)
              .add("infCT", ArrayType(new StructType()
                .add("dEmi", StringType, true)
                .add("nCT", StringType, true)
                .add("serie", StringType, true)
                .add("vCarga", DoubleType, true)), true)
              .add("infCTe", ArrayType(new StructType()
                .add("SegCodBarra", StringType, true)
                .add("chCTe", StringType, true)
                .add("indReentrega", StringType, true)
                .add("infEntregaParcial", new StructType()
                  .add("qtdParcial", DoubleType, true)
                  .add("qtdTotal", DoubleType, true))
                .add("infUnidTransp", ArrayType(new StructType()
                  .add("idUnidTransp", StringType, true)
                  .add("infUnidCarga", new StructType()
                    .add("idUnidCarga", StringType, true)
                    .add("lacUnidCarga", ArrayType(new StructType()
                      .add("nLacre", StringType, true)), true)
                    .add("qtdRat", DoubleType, true)
                    .add("tpUnidCarga", StringType, true))
                  .add("lacUnidTransp", ArrayType(new StructType()
                    .add("nLacre", StringType, true)), true)
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidTransp", StringType, true)), true)
                .add("peri", ArrayType(new StructType()
                  .add("grEmb", StringType, true)
                  .add("nONU", StringType, true)
                  .add("qTotProd", StringType, true)
                  .add("qVolTipo", StringType, true)
                  .add("xClaRisco", StringType, true)
                  .add("xNomeAE", StringType, true)), true)), true)
              .add("infNF", new StructType()
                .add("CNPJ", StringType, true)
                .add("UF", StringType, true)
                .add("dEmi", StringType, true)
                .add("infUnidTransp", new StructType()
                  .add("idUnidTransp", StringType, true)
                  .add("lacUnidTransp", ArrayType(new StructType()
                    .add("nLacre", StringType, true)), true)
                  .add("tpUnidTransp", StringType, true))
                .add("nNF", StringType, true)
                .add("serie", StringType, true)
                .add("vNF", DoubleType, true))
              .add("infNFe", ArrayType(new StructType()
                .add("SegCodBarra", StringType, true)
                .add("chNFe", StringType, true)
                .add("indReentrega", StringType, true)
                .add("infUnidTransp", ArrayType(new StructType()
                  .add("idUnidTransp", StringType, true)
                  .add("infUnidCarga", ArrayType(new StructType()
                    .add("idUnidCarga", StringType, true)
                    .add("lacUnidCarga", ArrayType(new StructType()
                      .add("nLacre", StringType, true)), true)
                    .add("qtdRat", DoubleType, true)
                    .add("tpUnidCarga", StringType, true)), true)
                  .add("lacUnidTransp", ArrayType(new StructType()
                    .add("nLacre", StringType, true)), true)
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidTransp", StringType, true)), true)
                .add("peri", ArrayType(new StructType()
                  .add("grEmb", StringType, true)
                  .add("nONU", StringType, true)
                  .add("qTotProd", StringType, true)
                  .add("qVolTipo", StringType, true)
                  .add("xClaRisco", StringType, true)
                  .add("xNomeAE", StringType, true)), true)), true)
              .add("xMunDescarga", StringType, true)), true))
          .add("infModal", new StructType()
            .add("_versaoModal", StringType, true)
            .add("aereo", new StructType()
              .add("cAerDes", StringType, true)
              .add("cAerEmb", StringType, true)
              .add("dVoo", StringType, true)
              .add("matr", StringType, true)
              .add("nVoo", StringType, true)
              .add("nac", StringType, true))
            .add("aquav", new StructType()
              .add("CNPJAgeNav", StringType, true)
              .add("cEmbar", StringType, true)
              .add("cPrtDest", StringType, true)
              .add("cPrtEmb", StringType, true)
              .add("infTermCarreg", new StructType()
                .add("cTermCarreg", StringType, true)
                .add("xTermCarreg", StringType, true))
              .add("infTermDescarreg", new StructType()
                .add("cTermDescarreg", StringType, true)
                .add("xTermDescarreg", StringType, true))
              .add("irin", StringType, true)
              .add("nViag", StringType, true)
              .add("prtTrans", StringType, true)
              .add("tpEmb", StringType, true)
              .add("tpNav", StringType, true)
              .add("xEmbar", StringType, true))
            .add("ferrov", new StructType()
              .add("trem", new StructType()
                .add("qVag", StringType, true)
                .add("xDest", StringType, true)
                .add("xOri", StringType, true)
                .add("xPref", StringType, true))
              .add("vag", ArrayType(new StructType()
                .add("TU", StringType, true)
                .add("nVag", StringType, true)
                .add("pesoBC", DoubleType, true)
                .add("pesoR", DoubleType, true)
                .add("serie", StringType, true)
                .add("tpVag", StringType, true)), true))
            .add("rodo", new StructType()
              .add("CIOT", StringType, true)
              .add("RNTRC", StringType, true)
              .add("codAgPorto", StringType, true)
              .add("infANTT", new StructType()
                .add("RNTRC", StringType, true)
                .add("infCIOT", ArrayType(new StructType()
                  .add("CIOT", StringType, true)
                  .add("CNPJ", StringType, true)
                  .add("CPF", StringType, true)), true)
                .add("infContratante", ArrayType(new StructType()
                  .add("CNPJ", StringType, true)
                  .add("CPF", StringType, true)
                  .add("idEstrangeiro", StringType, true)
                  .add("infContrato", new StructType()
                    .add("NroContrato", StringType, true)
                    .add("vContratoGlobal", DoubleType, true))
                  .add("xNome", StringType, true)), true)
                .add("infPag", ArrayType(new StructType()
                  .add("CNPJ", StringType, true)
                  .add("CPF", StringType, true)
                  .add("Comp", ArrayType(new StructType()
                    .add("tpComp", StringType, true)
                    .add("vComp", DoubleType, true)
                    .add("xComp", StringType, true)), true)
                  .add("indAltoDesemp", StringType, true)
                  .add("indAntecipaAdiant", StringType, true)
                  .add("indPag", StringType, true)
                  .add("infBanc", new StructType()
                    .add("CNPJIPEF", StringType, true)
                    .add("PIX", StringType, true)
                    .add("codAgencia", StringType, true)
                    .add("codBanco", StringType, true))
                  .add("infPrazo", ArrayType(new StructType()
                    .add("dVenc", StringType, true)
                    .add("nParcela", StringType, true)
                    .add("vParcela", DoubleType, true)), true)
                  .add("vAdiant", DoubleType, true)
                  .add("vContrato", DoubleType, true)
                  .add("xNome", StringType, true)), true))
              .add("valePed", new StructType()
                .add("categCombVeic", StringType, true)
                .add("disp", ArrayType(new StructType()
                  .add("CNPJForn", StringType, true)
                  .add("CNPJPg", StringType, true)
                  .add("CPFPg", StringType, true)
                  .add("nCompra", DoubleType, true)
                  .add("tpValePed", StringType, true)
                  .add("vValePed", StringType, true)), true))
              .add("lacRodo", ArrayType(new StructType()
                .add("nLacre", StringType, true)), true)
              .add("veicPrincipal", new StructType()
                .add("cInt", StringType, true)
                .add("capKG", StringType, true)
                .add("capM3", StringType, true)
                .add("condutor", new StructType()
                  .add("CPF", StringType, true)
                  .add("xNome", StringType, true))
                .add("placa", StringType, true)
                .add("prop", new StructType()
                  .add("RNTRC", StringType, true))
                .add("tara", StringType, true))
              .add("veicReboque", ArrayType(new StructType()
                .add("RENAVAM", StringType, true)
                .add("UF", StringType, true)
                .add("cInt", StringType, true)
                .add("capKG", StringType, true)
                .add("capM3", StringType, true)
                .add("placa", StringType, true)
                .add("prop", new StructType()
                  .add("CNPJ", StringType, true)
                  .add("CPF", StringType, true)
                  .add("IE", StringType, true)
                  .add("RNTRC", StringType, true)
                  .add("UF", StringType, true)
                  .add("tpProp", StringType, true)
                  .add("xNome", StringType, true))
                .add("tara", StringType, true)
                .add("tpCar", StringType, true)), true)
              .add("veicTracao", new StructType()
                .add("RENAVAM", StringType, true)
                .add("UF", StringType, true)
                .add("cInt", StringType, true)
                .add("capKG", StringType, true)
                .add("capM3", StringType, true)
                .add("condutor", ArrayType(new StructType()
                  .add("CPF", StringType, true)
                  .add("xNome", StringType, true)), true)
                .add("placa", StringType, true)
                .add("prop", new StructType()
                  .add("CNPJ", StringType, true)
                  .add("CPF", StringType, true)
                  .add("IE", StringType, true)
                  .add("RNTRC", StringType, true)
                  .add("UF", StringType, true)
                  .add("tpProp", StringType, true)
                  .add("xNome", StringType, true))
                .add("tara", StringType, true)
                .add("tpCar", StringType, true)
                .add("tpRod", StringType, true))))
          .add("infRespTec", new StructType()
            .add("CNPJ", StringType, true)
            .add("email", StringType, true)
            .add("fone", StringType, true)
            .add("hashCSRT", StringType, true)
            .add("idCSRT", StringType, true)
            .add("xContato", StringType, true))
          .add("lacres", ArrayType(new StructType()
            .add("nLacre", StringType, true)), true)
          .add("prodPred", new StructType()
            .add("NCM", StringType, true)
            .add("cEAN", StringType, true)
            .add("infLotacao", new StructType()
              .add("infLocalCarrega", new StructType()
                .add("CEP", StringType, true)
                .add("latitude", StringType, true)
                .add("longitude", StringType, true))
              .add("infLocalDescarrega", new StructType()
                .add("CEP", StringType, true)
                .add("latitude", StringType, true)
                .add("longitude", StringType, true)))
            .add("tpCarga", StringType, true)
            .add("xProd", StringType, true))
          .add("seg", ArrayType(new StructType()
            .add("infResp", new StructType()
              .add("CNPJ", StringType, true)
              .add("CPF", StringType, true)
              .add("respSeg", StringType, true))
            .add("infSeg", new StructType()
              .add("CNPJ", StringType, true)
              .add("xSeg", StringType, true))
            .add("nApol", StringType, true)
            .add("nAver", ArrayType(StringType, true)), true))
          .add("tot", new StructType()
            .add("cUnid", StringType, true)
            .add("qCT", StringType, true)
            .add("qCTe", StringType, true)
            .add("qCarga", DoubleType, true)
            .add("qMDFe", StringType, true)
            .add("qNF", StringType, true)
            .add("qNFe", StringType, true)
            .add("vCarga", DoubleType, true)))
        .add("infMDFeSupl", new StructType()
          .add("qrCodMDFe", StringType, true)))
      .add("_dhConexao", StringType, true)
      .add("_ipTransmissor", StringType, true)
      .add("_nPortaCon", StringType, true)
      .add("_versao", StringType, true)
      .add("_xmlns", StringType, true)
      .add("protMDFe", new StructType()
        .add("_versao", StringType, true)
        .add("infProt", new StructType()
          .add("_Id", StringType, true)
          .add("cStat", StringType, true)
          .add("chMDFe", StringType, true)
          .add("dhRecbto", StringType, true)
          .add("digVal", StringType, true)
          .add("nProt", StringType, true)
          .add("tpAmb", StringType, true)
          .add("verAplic", StringType, true)
          .add("xMotivo", StringType, true)))  }
}