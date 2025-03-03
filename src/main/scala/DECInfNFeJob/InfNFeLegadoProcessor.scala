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
package DECInfNFeJob

import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, _}

import java.time.LocalDateTime

object InfNFeLegadoProcessor {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val ano = 2025
  val mesInicio = 2
  val mesFim = 2
  val tipoDocumento = "nfe"

  // Função para criar o esquema de forma modular

  def createSchema(): StructType = {
    // Insira o esquema aqui
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
                .add("AAMM", LongType, nullable = true)
                .add("CNPJ", StringType, nullable = true)
                .add("cUF", StringType, nullable = true)
                .add("mod", StringType, nullable = true)
                .add("nNF", StringType, nullable = true)
                .add("serie", StringType, nullable = true)
              )
              .add("refNFP", new StructType()
                .add("AAMM", LongType, nullable = true)
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
              .add("dVenc", DateType, nullable = true)
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
            .add("CFOP", LongType, nullable = true)
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
            .add("dEmi", DateType, nullable = true)
            .add("dPag", DateType, nullable = true)
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
              .add("dPag", DateType, nullable = true)
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
              .add("CFOP", LongType, nullable = true)
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
              .add("dCompet", DateType, nullable = true)
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
      )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val schema = createSchema()
    // Lista de meses com base nas variáveis externas
    val anoMesList = (mesInicio to mesFim).map { month =>
      f"$ano${month}%02d"
    }.toList

    anoMesList.foreach { anoMes =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/$tipoDocumento/$anoMes"
      // Registrar o horário de início da iteração
      val startTime = LocalDateTime.now()
      println(s"Início da iteração para $anoMes: $startTime")
      println(s"Lendo dados do caminho: $parquetPath")
      // 1. Carrega o arquivo Parquet
      val parquetDF = spark.read.parquet(parquetPath)

      // 2. Seleciona as colunas XML_DOCUMENTO_CLOB e NSUDF
      val xmlDF = parquetDF.select(
        $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
        $"NSUDF".cast("string").as("NSUDF"),
        $"DHPROC",
        $"DHEMI",
        $"IP_TRANSMISSOR"
      )

      // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
      val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))
      //     parsedDF.printSchema()

      // 4. Seleciona os campos desejados
      val selectedDF = parsedDF.select(
        $"NSUDF",
        date_format(to_timestamp($"DHPROC", "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH").as("DHPROC_FORMATADO"),
        $"DHEMI",
        $"IP_TRANSMISSOR",
        $"parsed.protNFe.infProt._Id".as("infprot_Id"),
        $"parsed.protNFe.infProt.chNFe".as("chave"),
        $"parsed.protNFe.infProt.cStat".as("infprot_cstat"),
        $"parsed.protNFe.infProt.dhRecbto".as("infprot_dhrecbto"),
        $"parsed.protNFe.infProt.digVal".as("infprot_digVal"),
        $"parsed.protNFe.infProt.nProt".as("infprot_nProt"),
        $"parsed.protNFe.infProt.tpAmb".as("infprot_tpAmb"),
        $"parsed.protNFe.infProt.verAplic".as("infprot_verAplic"),
        $"parsed.protNFe.infProt.xMotivo".as("infprot_xMotivo"),
        $"parsed.NFe.infNFe.avulsa.CNPJ".as("avulsa_cnpj"),
        $"parsed.NFe.infNFe.avulsa.UF".as("avulsa_uf"),
        $"parsed.NFe.infNFe.avulsa.dEmi".as("avulsa_demi"),
        $"parsed.NFe.infNFe.avulsa.dPag".as("avulsa_dpag"),
        $"parsed.NFe.infNFe.avulsa.fone".as("avulsa_fone"),
        $"parsed.NFe.infNFe.avulsa.matr".as("avulsa_matr"),
        $"parsed.NFe.infNFe.avulsa.nDAR".as("avulsa_ndar"),
        $"parsed.NFe.infNFe.avulsa.repEmi".as("avulsa_repemi"),
        $"parsed.NFe.infNFe.avulsa.vDAR".as("avulsa_vdar"),
        $"parsed.NFe.infNFe.avulsa.xAgente".as("avulsa_xagente"),
        $"parsed.NFe.infNFe.avulsa.xOrgao".as("avulsa_xorgao"),
        $"parsed.NFe.infNFe.cobr.dup".as("cobr_dup"),
        $"parsed.NFe.infNFe.cobr.fat.nFat".as("cobr_fat_nfat"),
        $"parsed.NFe.infNFe.cobr.fat.vDesc".as("cobr_fat_vdesc"),
        $"parsed.NFe.infNFe.cobr.fat.vLiq".as("cobr_fat_vliq"),
        $"parsed.NFe.infNFe.cobr.fat.vOrig".as("cobr_fat_vorig"),
        $"parsed.NFe.infNFe.compra.xCont".as("compra_xcont"),
        $"parsed.NFe.infNFe.compra.xNEmp".as("compra_xnemp"),
        $"parsed.NFe.infNFe.compra.xPed".as("compra_xped"),
        $"parsed.NFe.infNFe.dest.CNPJ".as("dest_cnpj"),
        $"parsed.NFe.infNFe.dest.CPF".as("dest_cpf"),
        $"parsed.NFe.infNFe.dest.IE".as("dest_ie"),
        $"parsed.NFe.infNFe.dest.IM".as("dest_im"),
        $"parsed.NFe.infNFe.dest.ISUF".as("dest_isuf"),
        $"parsed.NFe.infNFe.dest.email".as("dest_email"),
        $"parsed.NFe.infNFe.dest.enderDest.CEP".as("enderdest_cep"),
        $"parsed.NFe.infNFe.dest.enderDest.UF".as("enderdest_uf"),
        $"parsed.NFe.infNFe.dest.enderDest.cMun".as("enderdest_cmun"),
        $"parsed.NFe.infNFe.dest.enderDest.cPais".as("enderdest_cpais"),
        $"parsed.NFe.infNFe.dest.enderDest.fone".as("enderdest_fone"),
        $"parsed.NFe.infNFe.dest.enderDest.nro".as("enderdest_nro"),
        $"parsed.NFe.infNFe.dest.enderDest.xBairro".as("enderdest_xbairro"),
        $"parsed.NFe.infNFe.dest.enderDest.xCpl".as("enderdest_xcpl"),
        $"parsed.NFe.infNFe.dest.enderDest.xLgr".as("enderdest_xlgr"),
        $"parsed.NFe.infNFe.dest.enderDest.xMun".as("enderdest_xmun"),
        $"parsed.NFe.infNFe.dest.enderDest.xPais".as("enderdest_xpais"),
        $"parsed.NFe.infNFe.dest.xNome".as("dest_xnome"),
        $"parsed.NFe.infNFe.dest.idEstrangeiro".as("idEstrangeiro"),
        $"parsed.NFe.infNFe.dest.indIEDest".as("indIEDest"),
        $"parsed.NFe.infNFe.emit.CNPJ".as("cnpj_emitente"),
        $"parsed.NFe.infNFe.emit.CPF".as("cpf_emitente"),
        $"parsed.NFe.infNFe.emit.CNPJ".as("emit_cnpj"),
        $"parsed.NFe.infNFe.emit.CPF".as("emit_cpf"),
        $"parsed.NFe.infNFe.emit.CNAE".as("emit_cnae"),
        $"parsed.NFe.infNFe.emit.CRT".as("emit_crt"),
        $"parsed.NFe.infNFe.emit.IE".as("emit_ie"),
        $"parsed.NFe.infNFe.emit.IEST".as("emit_iest"),
        $"parsed.NFe.infNFe.emit.IM".as("emit_im"),
        $"parsed.NFe.infNFe.emit.enderEmit.CEP".as("enderemit_cep"),
        $"parsed.NFe.infNFe.emit.enderEmit.UF".as("enderemit_uf"),
        $"parsed.NFe.infNFe.emit.enderEmit.cMun".as("enderemit_cmun"),
        $"parsed.NFe.infNFe.emit.enderEmit.cPais".as("enderemit_cpais"),
        $"parsed.NFe.infNFe.emit.enderEmit.fone".as("enderemit_fone"),
        $"parsed.NFe.infNFe.emit.enderEmit.nro".as("enderemit_nro"),
        $"parsed.NFe.infNFe.emit.enderEmit.xBairro".as("enderemit_xbairro"),
        $"parsed.NFe.infNFe.emit.enderEmit.xCpl".as("enderemit_xcpl"),
        $"parsed.NFe.infNFe.emit.enderEmit.xLgr".as("enderemit_xlgr"),
        $"parsed.NFe.infNFe.emit.enderEmit.xMun".as("enderemit_xmun"),
        $"parsed.NFe.infNFe.emit.enderEmit.xPais".as("enderemit_xpais"),
        $"parsed.NFe.infNFe.emit.xFant".as("emit_xfant"),
        $"parsed.NFe.infNFe.emit.xNome".as("emit_xnome"),
        $"parsed.NFe.infNFe.entrega.CEP".as("entrega_cep"),
        $"parsed.NFe.infNFe.entrega.CNPJ".as("entrega_cnpj"),
        $"parsed.NFe.infNFe.entrega.IE".as("entrega_ie"),
        $"parsed.NFe.infNFe.entrega.CPF".as("entrega_cpf"),
        $"parsed.NFe.infNFe.entrega.UF".as("entrega_uf"),
        $"parsed.NFe.infNFe.entrega.cMun".as("entrega_cmun"),
        $"parsed.NFe.infNFe.entrega.cPais".as("entrega_cpais"),
        $"parsed.NFe.infNFe.entrega.email".as("entrega_email"),
        $"parsed.NFe.infNFe.entrega.fone".as("entrega_fone"),
        $"parsed.NFe.infNFe.entrega.nro".as("entrega_nro"),
        $"parsed.NFe.infNFe.entrega.xBairro".as("entrega_xbairro"),
        $"parsed.NFe.infNFe.entrega.xCpl".as("entrega_xcpl"),
        $"parsed.NFe.infNFe.entrega.xLgr".as("entrega_xlgr"),
        $"parsed.NFe.infNFe.entrega.xMun".as("entrega_xmun"),
        $"parsed.NFe.infNFe.entrega.xNome".as("entrega_xnome"),
        $"parsed.NFe.infNFe.entrega.xPais".as("entrega_xpais"),
        $"parsed.NFe.infNFe.exporta.UFSaidaPais".as("exporta_ufsaidapais"),
        $"parsed.NFe.infNFe.exporta.xLocDespacho".as("exporta_xlocdespacho"),
        $"parsed.NFe.infNFe.exporta.xLocExporta".as("exporta_xlocexporta"),
        $"parsed.NFe.infNFe.ide.dhEmi".as("ide_dhemi"),
        $"parsed.NFe.infNFe.ide.cDV".as("ide_cdv"),
        $"parsed.NFe.infNFe.ide.cMunFG".as("ide_cmungfg"),
        $"parsed.NFe.infNFe.ide.cNF".as("ide_cnf"),
        $"parsed.NFe.infNFe.ide.cUF".as("ide_cuf"),
        $"parsed.NFe.infNFe.ide.dhCont".as("ide_dhcont"),
        $"parsed.NFe.infNFe.ide.dhSaiEnt".as("ide_dhsaient"),
        $"parsed.NFe.infNFe.ide.finNFe".as("ide_finnfe"),
        $"parsed.NFe.infNFe.ide.idDest".as("ide_iddest"),
        $"parsed.NFe.infNFe.ide.indFinal".as("ide_indfinal"),
        $"parsed.NFe.infNFe.ide.indIntermed".as("ide_indintermed"),
        $"parsed.NFe.infNFe.ide.indPres".as("ide_indpres"),
        $"parsed.NFe.infNFe.ide.mod".as("ide_mod"),
        $"parsed.NFe.infNFe.ide.nNF".as("ide_nnfe"),
        $"parsed.NFe.infNFe.ide.natOp".as("ide_natop"),
        $"parsed.NFe.infNFe.ide.procEmi".as("ide_procemi"),
        $"parsed.NFe.infNFe.ide.serie".as("ide_serie"),
        $"parsed.NFe.infNFe.ide.tpAmb".as("ide_tpamb"),
        $"parsed.NFe.infNFe.ide.tpEmis".as("ide_tpemis"),
        $"parsed.NFe.infNFe.ide.tpImp".as("ide_tpimp"),
        $"parsed.NFe.infNFe.ide.tpNF".as("ide_tpnf"),
        $"parsed.NFe.infNFe.ide.verProc".as("ide_verproc"),
        $"parsed.NFe.infNFe.ide.xJust".as("ide_xjust"),
        $"parsed.NFe.infNFe.ide.NFref".as("ide_nfref"),
        $"parsed.NFe.infNFe.retirada.CEP".as("retirada_cep"),
        $"parsed.NFe.infNFe.retirada.CNPJ".as("retirada_cnpj"),
        $"parsed.NFe.infNFe.retirada.CPF".as("retirada_cpf"),
        $"parsed.NFe.infNFe.retirada.IE".as("retirada_ie"),
        $"parsed.NFe.infNFe.retirada.UF".as("retirada_uf"),
        $"parsed.NFe.infNFe.retirada.cMun".as("retirada_cmun"),
        $"parsed.NFe.infNFe.retirada.cPais".as("retirada_cpais"),
        $"parsed.NFe.infNFe.retirada.email".as("retirada_email"),
        $"parsed.NFe.infNFe.retirada.fone".as("retirada_fone"),
        $"parsed.NFe.infNFe.retirada.nro".as("retirada_nro"),
        $"parsed.NFe.infNFe.retirada.xBairro".as("retirada_xbairro"),
        $"parsed.NFe.infNFe.retirada.xCpl".as("retirada_xcpl"),
        $"parsed.NFe.infNFe.retirada.xLgr".as("retirada_xlgr"),
        $"parsed.NFe.infNFe.retirada.xMun".as("retirada_xmun"),
        $"parsed.NFe.infNFe.retirada.xNome".as("retirada_xnome"),
        $"parsed.NFe.infNFe.retirada.xPais".as("retirada_xpais"),
        $"parsed.NFe.infNFe.retirada.modFrete".as("retirada_modfrete"),
        $"parsed.NFe.infNFe.retTransp.cfop".as("rettransp_cfop"),
        $"parsed.NFe.infNFe.retTransp.cmunfg".as("rettransp_cmunfg"),
        $"parsed.NFe.infNFe.retTransp.picmsret".as("rettransp_picmsret"),
        $"parsed.NFe.infNFe.retTransp.vbcret".as("rettransp_vbcret"),
        $"parsed.NFe.infNFe.retTransp.vicmsret".as("rettransp_vicmsret"),
        $"parsed.NFe.infNFe.retTransp.vserv".as("rettransp_vserv"),
        $"parsed.NFe.infNFe.total.icmstot.qbcmono".as("icmstot_qbcmono"),
        $"parsed.NFe.infNFe.total.icmstot.qbcmonoret".as("icmstot_qbcmonoret"),
        $"parsed.NFe.infNFe.total.icmstot.qbcmonoreten".as("icmstot_qbcmonoreten"),
        $"parsed.NFe.infNFe.total.icmstot.vbc".as("icmstot_vbc"),
        $"parsed.NFe.infNFe.total.icmstot.vbcst".as("icmstot_vbcst"),
        $"parsed.NFe.infNFe.total.icmstot.vcofins".as("icmstot_vcofins"),
        $"parsed.NFe.infNFe.total.icmstot.vdesc".as("icmstot_vdesc"),
        $"parsed.NFe.infNFe.total.icmstot.vfcp".as("icmstot_vfcp"),
        $"parsed.NFe.infNFe.total.icmstot.vfcpst".as("icmstot_vfcpst"),
        $"parsed.NFe.infNFe.total.icmstot.vfcpstret".as("icmstot_vfcpstret"),
        $"parsed.NFe.infNFe.total.icmstot.vfcpufdest".as("icmstot_vfcpufdest"),
        $"parsed.NFe.infNFe.total.icmstot.vfrete".as("icmstot_vfrete"),
        $"parsed.NFe.infNFe.total.icmstot.vicms".as("icmstot_vicms"),
        $"parsed.NFe.infNFe.total.icmstot.vicmsdeson".as("icmstot_vicmsdeson"),
        $"parsed.NFe.infNFe.total.icmstot.vicmsmono".as("icmstot_vicmsmono"),
        $"parsed.NFe.infNFe.total.icmstot.vicmsmonoret".as("icmstot_vicmsmonoret"),
        $"parsed.NFe.infNFe.total.icmstot.vicmsmonoreten".as("icmstot_vicmsmonoreten"),
        $"parsed.NFe.infNFe.total.icmstot.vicmsufdest".as("icmstot_vicmsufdest"),
        $"parsed.NFe.infNFe.total.icmstot.vicmsufremet".as("icmstot_vicmsufremet"),
        $"parsed.NFe.infNFe.total.icmstot.vii".as("icmstot_vii"),
        $"parsed.NFe.infNFe.total.icmstot.vipi".as("icmstot_vipi"),
        $"parsed.NFe.infNFe.total.icmstot.vipidevol".as("icmstot_vipidevol"),
        $"parsed.NFe.infNFe.total.icmstot.vnf".as("icmstot_vnf"),
        $"parsed.NFe.infNFe.total.icmstot.voutro".as("icmstot_voutro"),
        $"parsed.NFe.infNFe.total.icmstot.vpis".as("icmstot_vpis"),
        $"parsed.NFe.infNFe.total.icmstot.vprod".as("icmstot_vprod"),
        $"parsed.NFe.infNFe.total.icmstot.vst".as("icmstot_vst"),
        $"parsed.NFe.infNFe.total.icmstot.vseg".as("icmstot_vseg"),
        $"parsed.NFe.infNFe.total.icmstot.vtottrib".as("icmstot_vtottrib"),
        $"parsed.NFe.infNFe.pag.detPag".as("pag_detPag"),
        $"parsed.NFe.infNFe.pag.vTroco".as("pag_vtroco"),
        $"parsed.NFe.infNFe.transp.modFrete".as("transp_modFrete"),
        $"parsed.NFe.infNFe.transp.reboque".as("transp_reboque"),
        $"parsed.NFe.infNFe.transp.retTransp.CFOP".as("transp_retTransp_CFOP"),
        $"parsed.NFe.infNFe.transp.retTransp.cMunFG".as("transp_retTransp_cMunFG"),
        $"parsed.NFe.infNFe.transp.retTransp.pICMSRet".as("transp_retTransp_pICMSRet"),
        $"parsed.NFe.infNFe.transp.retTransp.vBCRet".as("transp_retTransp_vBCRet"),
        $"parsed.NFe.infNFe.transp.retTransp.vICMSRet".as("transp_retTransp_vICMSRet"),
        $"parsed.NFe.infNFe.transp.retTransp.vServ".as("transp_retTransp_vServ"),
        $"parsed.NFe.infNFe.transp.transporta.CNPJ".as("transp_transporta_CNPJ"),
        $"parsed.NFe.infNFe.transp.transporta.CPF".as("transp_transporta_CPF"),
        $"parsed.NFe.infNFe.transp.transporta.IE".as("transp_transporta_IE"),
        $"parsed.NFe.infNFe.transp.transporta.UF".as("transp_transporta_UF"),
        $"parsed.NFe.infNFe.transp.transporta.xEnder".as("transp_transporta_xEnder"),
        $"parsed.NFe.infNFe.transp.transporta.xMun".as("transp_transporta_xMunJ"),
        $"parsed.NFe.infNFe.transp.transporta.xNome".as("transp_transporta_xNome"),
        $"parsed.NFe.infNFe.transp.transporta".as("transp_transporta"),
        $"parsed.NFe.infNFe.transp.vagao".as("transp_vagao"),
        $"parsed.NFe.infNFe.transp.veicTransp.RNTC".as("transp_veicTransp_RNTC"),
        $"parsed.NFe.infNFe.transp.veicTransp.UF".as("transp_veicTransp_UF"),
        $"parsed.NFe.infNFe.transp.veicTransp.placa".as("transp_veicTransp_placa"),
        $"parsed.NFe.infNFe.transp.vol".as("transp_vol"),
        $"parsed.NFe.infNFe.infAdic.infAdFisco".as("infadic_infadfisco"),
        $"parsed.NFe.infNFe.infAdic.infCpl".as("infadic_infcpl"),
        $"parsed.NFe.infNFe.infAdic.obsCont".as("infadic_obscont"), // Array de structs
        $"parsed.NFe.infNFe.infAdic.obsFisco".as("infadic_obsfisco"), // Array de structs
        $"parsed.NFe.infNFe.infAdic.procRef".as("infadic_procref"), // Array de structs
        $"parsed.NFe.infNFe.infIntermed.CNPJ".as("infintermed_cnpj"),
        $"parsed.NFe.infNFe.infIntermed.idCadIntTran".as("infintermed_idcadinttran"),
        $"parsed.NFe.infNFe.infRespTec.CNPJ".as("infresptec_cnpj"),
        $"parsed.NFe.infNFe.infRespTec.email".as("infresptec_email"),
        $"parsed.NFe.infNFe.infRespTec.fone".as("infresptec_fone"),
        $"parsed.NFe.infNFe.infRespTec.hashCSRT".as("infresptec_hashcsrt"),
        $"parsed.NFe.infNFe.infRespTec.idCSRT".as("infresptec_idcsrt"),
        $"parsed.NFe.infNFe.infRespTec.xContato".as("infresptec_xcontato"),
        $"parsed.NFe.infNFe.infSolicNFF.xSolic".as("infsolicnff_xsolic")
      )
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
        .save("/datalake/prata/sources/dbms/dec/nfe/infNFe")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")    }
  }
}

//InfNFeLegadoProcessor.main(Array())