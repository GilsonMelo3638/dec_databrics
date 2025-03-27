package Schemas
import org.apache.spark.sql.types._

object NFCeSchema {
  def createSchema(): StructType = {
    new StructType()
      .add("protNFe", new StructType()
        .add("infProt", new StructType()
          .add("_Id", StringType, nullable = true)
          .add("chNFe", StringType, nullable = true)
          .add("cStat", StringType, nullable = true)
          .add("dhRecbto", StringType, nullable = true)
          .add("digVal", StringType, nullable = true)
          .add("nProt", StringType, nullable = true)
          .add("tpAmb", StringType, nullable = true)
          .add("verAplic", StringType, nullable = true)
          .add("xMotivo", StringType, nullable = true)
        )
      )
      .add("NFe", new StructType()
        .add("infNFe", new StructType()
          .add("ide", new StructType()
            .add("dhEmi", StringType, nullable = true)
            .add("cDV", StringType, nullable = true)
            .add("cMunFG", StringType, nullable = true)
            .add("cNF", StringType, nullable = true)
            .add("cUF", StringType, nullable = true)
            .add("dhCont", StringType, nullable = true)
            .add("dhSaiEnt", StringType, nullable = true)
            .add("finNFe", StringType, nullable = true)
            .add("idDest", StringType, nullable = true)
            .add("indFinal", StringType, nullable = true)
            .add("indIntermed", StringType, nullable = true)
            .add("indPres", StringType, nullable = true)
            .add("mod", StringType, nullable = true)
            .add("nNF", StringType, nullable = true)
            .add("natOp", StringType, nullable = true)
            .add("procEmi", StringType, nullable = true)
            .add("serie", StringType, nullable = true)
            .add("tpAmb", StringType, nullable = true)
            .add("tpEmis", StringType, nullable = true)
            .add("tpImp", StringType, nullable = true)
            .add("tpNF", StringType, nullable = true)
            .add("verProc", StringType, nullable = true)
            .add("xJust", StringType, nullable = true)
            .add("NFref", ArrayType(new StructType()
              .add("refCTe", StringType, nullable = true)
              .add("refECF", new StructType()
                .add("mod", StringType, nullable = true)
                .add("nCOO", StringType, nullable = true)
                .add("nECF", StringType, nullable = true)
              )
              .add("refNF", new StructType()
                .add("AAMM", StringType, nullable = true)
                .add("CNPJ", StringType, nullable = true)
                .add("cUF", StringType, nullable = true)
                .add("mod", StringType, nullable = true)
                .add("nNF", StringType, nullable = true)
                .add("serie", StringType, nullable = true)
              )
              .add("refNFP", new StructType()
                .add("AAMM", StringType, nullable = true)
                .add("CPF", StringType, nullable = true)
                .add("IE", StringType, nullable = true)
                .add("cUF", StringType, nullable = true)
                .add("mod", StringType, nullable = true)
                .add("nNF", StringType, nullable = true)
                .add("serie", StringType, nullable = true)
              )
              .add("refNFe", StringType, nullable = true)
              .add("refNFeSig", StringType, nullable = true)
            ))
          )
          .add("cobr", new StructType()
            .add("dup", ArrayType(new StructType()
              .add("dVenc", StringType, nullable = true)
              .add("nDup", StringType, nullable = true)
              .add("vDup", DoubleType, nullable = true)
            ))
            .add("fat", new StructType()
              .add("nFat", StringType, nullable = true)
              .add("vDesc", DoubleType, nullable = true)
              .add("vLiq", DoubleType, nullable = true)
              .add("vOrig", DoubleType, nullable = true)
            )
          )
          .add("retTransp", new StructType()
            .add("CFOP", StringType, nullable = true)
            .add("cMunFG", StringType, nullable = true)
            .add("pICMSRet", DoubleType, nullable = true)
            .add("vBCRet", DoubleType, nullable = true)
            .add("vICMSRet", DoubleType, nullable = true)
            .add("vServ", DoubleType, nullable = true)
          )
          .add("transporta", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("CPF", StringType, nullable = true)
            .add("IE", StringType, nullable = true)
            .add("UF", StringType, nullable = true)
            .add("xEnder", StringType, nullable = true)
            .add("xMun", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
            .add("vagao", StringType, nullable = true)
            .add("veicTransp", new StructType()
              .add("RNTC", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
              .add("placa", StringType, nullable = true)
            )
          )
          .add("avulsa", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("UF", StringType, nullable = true)
            .add("dEmi", StringType, nullable = true)
            .add("dPag", StringType, nullable = true)
            .add("fone", StringType, nullable = true)
            .add("matr", StringType, nullable = true)
            .add("nDAR", StringType, nullable = true)
            .add("repEmi", StringType, nullable = true)
            .add("vDAR", DoubleType, nullable = true)
            .add("xAgente", StringType, nullable = true)
            .add("xOrgao", StringType, nullable = true)
          )
          .add("retirada", new StructType()
            .add("CEP", StringType, nullable = true)
            .add("CNPJ", StringType, nullable = true)
            .add("CPF", StringType, nullable = true)
            .add("IE", StringType, nullable = true)
            .add("UF", StringType, nullable = true)
            .add("cMun", StringType, nullable = true)
            .add("cPais", StringType, nullable = true)
            .add("email", StringType, nullable = true)
            .add("fone", StringType, nullable = true)
            .add("nro", StringType, nullable = true)
            .add("xBairro", StringType, nullable = true)
            .add("xCpl", StringType, nullable = true)
            .add("xLgr", StringType, nullable = true)
            .add("xMun", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
            .add("xPais", StringType, nullable = true)
            .add("modFrete", StringType, nullable = true)
          )
          .add("compra", new StructType()
            .add("xCont", StringType, nullable = true)
            .add("xNEmp", StringType, nullable = true)
            .add("xPed", StringType, nullable = true)
          )
          .add("dest", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("CPF", StringType, nullable = true)
            .add("IE", StringType, nullable = true)
            .add("IM", StringType, nullable = true)
            .add("ISUF", StringType, nullable = true)
            .add("email", StringType, nullable = true)
            .add("enderDest", new StructType()
              .add("CEP", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
              .add("cMun", StringType, nullable = true)
              .add("cPais", StringType, nullable = true)
              .add("fone", StringType, nullable = true)
              .add("nro", StringType, nullable = true)
              .add("xBairro", StringType, nullable = true)
              .add("xCpl", StringType, nullable = true)
              .add("xLgr", StringType, nullable = true)
              .add("xMun", StringType, nullable = true)
              .add("xPais", StringType, nullable = true)
            )
            .add("idEstrangeiro", StringType, nullable = true)
            .add("indIEDest", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
          )
          .add("pag", new StructType()
            .add("detPag", ArrayType(new StructType()
              .add("CNPJPag", StringType, nullable = true)
              .add("UFPag", StringType, nullable = true)
              .add("card", new StructType()
                .add("CNPJ", StringType, nullable = true)
                .add("CNPJReceb", StringType, nullable = true)
                .add("cAut", StringType, nullable = true)
                .add("idTermPag", StringType, nullable = true)
                .add("tBand", StringType, nullable = true)
                .add("tpIntegra", StringType, nullable = true)
              )
              .add("dPag", StringType, nullable = true)
              .add("indPag", StringType, nullable = true)
              .add("tPag", StringType, nullable = true)
              .add("vPag", DoubleType, nullable = true)
              .add("xPag", StringType, nullable = true)
            ))
            .add("vTroco", DoubleType, nullable = true)
          )
          .add("transp", new StructType()
            .add("modFrete", StringType, nullable = true)
            .add("reboque", ArrayType(new StructType()
              .add("RNTC", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
              .add("placa", StringType, nullable = true)
            ))
            .add("retTransp", new StructType()
              .add("CFOP", StringType, nullable = true)
              .add("cMunFG", StringType, nullable = true)
              .add("pICMSRet", DoubleType, nullable = true)
              .add("vBCRet", DoubleType, nullable = true)
              .add("vICMSRet", DoubleType, nullable = true)
              .add("vServ", DoubleType, nullable = true)
            )
            .add("transporta", new StructType()
              .add("CNPJ", StringType, nullable = true)
              .add("CPF", StringType, nullable = true)
              .add("IE", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
              .add("xEnder", StringType, nullable = true)
              .add("xMun", StringType, nullable = true)
              .add("xNome", StringType, nullable = true)
            )
            .add("vagao", StringType, nullable = true)
            .add("veicTransp", new StructType()
              .add("RNTC", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
              .add("placa", StringType, nullable = true)
            )
            .add("vol", ArrayType(new StructType()
              .add("esp", StringType, nullable = true)
              .add("lacres", ArrayType(new StructType()
                .add("nLacre", StringType, nullable = true)
              ))
              .add("marca", StringType, nullable = true)
              .add("nVol", StringType, nullable = true)
              .add("pesoB", DoubleType, nullable = true)
              .add("pesoL", DoubleType, nullable = true)
              .add("qVol", StringType, nullable = true)
            )))
          .add("entrega", new StructType()
            .add("CEP", StringType, nullable = true)
            .add("CNPJ", StringType, nullable = true)
            .add("IE", StringType, nullable = true)
            .add("CPF", StringType, nullable = true)
            .add("UF", StringType, nullable = true)
            .add("cMun", StringType, nullable = true)
            .add("cPais", StringType, nullable = true)
            .add("email", StringType, nullable = true)
            .add("fone", StringType, nullable = true)
            .add("nro", StringType, nullable = true)
            .add("xBairro", StringType, nullable = true)
            .add("xCpl", StringType, nullable = true)
            .add("xLgr", StringType, nullable = true)
            .add("xMun", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
            .add("xPais", StringType, nullable = true)
          )
          .add("exporta", new StructType()
            .add("UFSaidaPais", StringType, nullable = true)
            .add("xLocDespacho", StringType, nullable = true)
            .add("xLocExporta", StringType, nullable = true)
          )
          .add("emit", new StructType()
            .add("CNAE", StringType, nullable = true)
            .add("CNPJ", StringType, nullable = true)
            .add("CPF", StringType, nullable = true)
            .add("CRT", StringType, nullable = true)
            .add("IE", StringType, nullable = true)
            .add("IEST", StringType, nullable = true)
            .add("IM", StringType, nullable = true)
            .add("enderEmit", new StructType()
              .add("CEP", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
              .add("cMun", StringType, nullable = true)
              .add("cPais", StringType, nullable = true)
              .add("fone", StringType, nullable = true)
              .add("nro", StringType, nullable = true)
              .add("xBairro", StringType, nullable = true)
              .add("xCpl", StringType, nullable = true)
              .add("xLgr", StringType, nullable = true)
              .add("xMun", StringType, nullable = true)
              .add("xPais", StringType, nullable = true)
            )
            .add("xFant", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
          )
          .add("infAdic", new StructType()
            .add("infAdFisco", StringType, nullable = true)
            .add("infCpl", StringType, nullable = true)
            .add("obsCont", ArrayType(new StructType()
              .add("_xCampo", StringType, nullable = true)
              .add("xTexto", StringType, nullable = true)
            ))
            .add("obsFisco", ArrayType(new StructType()
              .add("_xCampo", StringType, nullable = true)
              .add("xTexto", StringType, nullable = true)
            ))
            .add("procRef", ArrayType(new StructType()
              .add("indProc", StringType, nullable = true)
              .add("nProc", StringType, nullable = true)
              .add("tpAto", StringType, nullable = true)
            ))
          )
          .add("infIntermed", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("idCadIntTran", StringType, nullable = true)
          )
          .add("infRespTec", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("email", StringType, nullable = true)
            .add("fone", StringType, nullable = true)
            .add("hashCSRT", StringType, nullable = true)
            .add("idCSRT", StringType, nullable = true)
            .add("xContato", StringType, nullable = true)
          )
          .add("infSolicNFF", new StructType()
            .add("xSolic", StringType, nullable = true)
          )
          .add("total", new StructType()
            .add("ICMSTot", new StructType()
              .add("qBCMono", DoubleType, nullable = true)
              .add("qBCMonoRet", DoubleType, nullable = true)
              .add("qBCMonoReten", DoubleType, nullable = true)
              .add("vBC", DoubleType, nullable = true)
              .add("vBCST", DoubleType, nullable = true)
              .add("vCOFINS", DoubleType, nullable = true)
              .add("vDesc", DoubleType, nullable = true)
              .add("vFCP", DoubleType, nullable = true)
              .add("vFCPST", DoubleType, nullable = true)
              .add("vFCPSTRet", DoubleType, nullable = true)
              .add("vFCPUFDest", DoubleType, nullable = true)
              .add("vFrete", DoubleType, nullable = true)
              .add("vICMS", DoubleType, nullable = true)
              .add("vICMSDeson", DoubleType, nullable = true)
              .add("vICMSMono", DoubleType, nullable = true)
              .add("vICMSMonoRet", DoubleType, nullable = true)
              .add("vICMSMonoReten", DoubleType, nullable = true)
              .add("vICMSUFDest", DoubleType, nullable = true)
              .add("vICMSUFRemet", DoubleType, nullable = true)
              .add("vII", DoubleType, nullable = true)
              .add("vIPI", DoubleType, nullable = true)
              .add("vIPIDevol", DoubleType, nullable = true)
              .add("vNF", DoubleType, nullable = true)
              .add("vOutro", DoubleType, nullable = true)
              .add("vPIS", DoubleType, nullable = true)
              .add("vProd", DoubleType, nullable = true)
              .add("vST", DoubleType, nullable = true)
              .add("vSeg", DoubleType, nullable = true)
              .add("vTotTrib", DoubleType, nullable = true)
            )
            .add("ISSQNtot", new StructType()
              .add("cRegTrib", StringType, nullable = true)
              .add("dCompet", StringType, nullable = true)
              .add("vBC", DoubleType, nullable = true)
              .add("vCOFINS", DoubleType, nullable = true)
              .add("vISS", DoubleType, nullable = true)
              .add("vPIS", DoubleType, nullable = true)
              .add("vServ", DoubleType, nullable = true)
            )
            .add("retTrib", new StructType()
              .add("vBCIRRF", DoubleType, nullable = true)
              .add("vBCRetPrev", DoubleType, nullable = true)
              .add("vIRRF", DoubleType, nullable = true)
              .add("vRetCOFINS", DoubleType, nullable = true)
              .add("vRetCSLL", DoubleType, nullable = true)
              .add("vRetPIS", DoubleType, nullable = true)
            )
          )
        )
        .add("infNFeSupl", new StructType() // Adicionando a estrutura infNFeSupl
          .add("qrCode", StringType, nullable = true)
          .add("urlChave", StringType, nullable = true)
        )
      )
  }
}