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

import java.time.LocalDateTime

object CTeLegadoProcessor {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val anoInicio = 2025
  val anoFim = 2025
  val tipoDocumento = "cte"

  // Função para criar o esquema de forma modular

  import org.apache.spark.sql.types._

  def createSchema(): StructType = {
    new StructType()
      .add("protCTe", new StructType()
        .add("_versao", DoubleType, true)
        .add("infProt", new StructType()
          .add("_Id", StringType, true)
          .add("cStat", LongType, true)
          .add("chCTe", StringType, true)
          .add("dhRecbto", TimestampType, true)
          .add("digVal", StringType, true)
          .add("nProt", LongType, true)
          .add("tpAmb", LongType, true)
          .add("verAplic", StringType, true)
          .add("xMotivo", StringType, true)))
      .add("CTe", new StructType()
        .add("infCTeSupl", new StructType()
          .add("qrCodCTe", StringType, true))
        .add("infCte", new StructType()
          .add("_Id", StringType, true)
          .add("_versao", DoubleType, true)
          .add("autXML", ArrayType(new StructType()
            .add("CNPJ", LongType, true)
            .add("CPF", LongType, true)), true)
          .add("compl", new StructType()
            .add("Entrega", new StructType()
              .add("comData", new StructType()
                .add("dProg", DateType, true)
                .add("tpPer", LongType, true))
              .add("comHora", new StructType()
                .add("hProg", StringType, true)
                .add("tpHor", LongType, true))
              .add("noInter", new StructType()
                .add("hFim", StringType, true)
                .add("hIni", StringType, true)
                .add("tpHor", LongType, true))
              .add("noPeriodo", new StructType()
                .add("dFim", DateType, true)
                .add("dIni", DateType, true)
                .add("tpPer", LongType, true))
              .add("semData", new StructType()
                .add("tpPer", LongType, true))
              .add("semHora", new StructType()
                .add("tpHor", LongType, true)))
            .add("ObsCont", ArrayType(new StructType()
              .add("_xCampo", StringType, true)
              .add("xTexto", StringType, true)), true)
            .add("ObsFisco", ArrayType(new StructType()
              .add("_xCampo", StringType, true)
              .add("xTexto", StringType, true)), true)
            .add("destCalc", StringType, true)
            .add("fluxo", new StructType()
              .add("pass", ArrayType(new StructType()
                .add("xPass", StringType, true)), true)
              .add("xDest", StringType, true)
              .add("xOrig", StringType, true)
              .add("xRota", StringType, true))
            .add("origCalc", StringType, true)
            .add("xCaracAd", StringType, true)
            .add("xCaracSer", StringType, true)
            .add("xEmi", StringType, true)
            .add("xObs", StringType, true))
          .add("dest", new StructType()
            .add("CNPJ", LongType, true)
            .add("CPF", LongType, true)
            .add("IE", StringType, true)
            .add("ISUF", LongType, true)
            .add("email", StringType, true)
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
            .add("fone", LongType, true)
            .add("xNome", StringType, true))
          .add("emit", new StructType()
            .add("CNPJ", LongType, true)
            .add("CPF", LongType, true)
            .add("CRT", LongType, true)
            .add("IE", LongType, true)
            .add("IEST", LongType, true)
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
          .add("exped", new StructType()
            .add("CNPJ", LongType, true)
            .add("CPF", LongType, true)
            .add("IE", StringType, true)
            .add("email", StringType, true)
            .add("enderExped", new StructType()
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
            .add("dhCont", TimestampType, true)
            .add("dhEmi", TimestampType, true)
            .add("indGlobalizado", LongType, true)
            .add("indIEToma", LongType, true)
            .add("mod", LongType, true)
            .add("modal", LongType, true)
            .add("nCT", LongType, true)
            .add("natOp", StringType, true)
            .add("procEmi", LongType, true)
            .add("retira", LongType, true)
            .add("serie", LongType, true)
            .add("toma3", new StructType()
              .add("toma", LongType, true))
            .add("toma4", new StructType()
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
              .add("toma", LongType, true)
              .add("xFant", StringType, true)
              .add("xNome", StringType, true))
            .add("tpAmb", LongType, true)
            .add("tpCTe", LongType, true)
            .add("tpEmis", LongType, true)
            .add("tpImp", LongType, true)
            .add("tpServ", LongType, true)
            .add("verProc", StringType, true)
            .add("xDetRetira", StringType, true)
            .add("xJust", StringType, true)
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
                .add("pICMS", DoubleType, true)
                .add("pRedBC", DoubleType, true)
                .add("vBC", DoubleType, true)
                .add("vICMS", DoubleType, true))
              .add("ICMS45", new StructType()
                .add("CST", LongType, true)
                .add("cBenef", StringType, true)
                .add("vICMSDeson", DoubleType, true))
              .add("ICMS60", new StructType()
                .add("CST", LongType, true)
                .add("cBenef", StringType, true)
                .add("pICMSSTRet", DoubleType, true)
                .add("vBCSTRet", DoubleType, true)
                .add("vCred", DoubleType, true)
                .add("vICMSDeson", DoubleType, true)
                .add("vICMSSTRet", DoubleType, true))
              .add("ICMS90", new StructType()
                .add("CST", LongType, true)
                .add("cBenef", LongType, true)
                .add("pICMS", DoubleType, true)
                .add("pRedBC", DoubleType, true)
                .add("vBC", DoubleType, true)
                .add("vCred", DoubleType, true)
                .add("vICMS", DoubleType, true)
                .add("vICMSDeson", DoubleType, true))
              .add("ICMSOutraUF", new StructType()
                .add("CST", LongType, true)
                .add("cBenef", StringType, true)
                .add("pICMSOutraUF", DoubleType, true)
                .add("pRedBCOutraUF", DoubleType, true)
                .add("vBCOutraUF", DoubleType, true)
                .add("vICMSDeson", DoubleType, true)
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
            .add("docAnt", new StructType()
              .add("emiDocAnt", ArrayType(new StructType()
                .add("CNPJ", LongType, true)
                .add("IE", LongType, true)
                .add("UF", StringType, true)
                .add("idDocAnt", ArrayType(new StructType()
                  .add("idDocAntEle", ArrayType(new StructType()
                    .add("chCTe", DoubleType, true)), true)
                  .add("idDocAntPap", ArrayType(new StructType()
                    .add("dEmi", DateType, true)
                    .add("nDoc", StringType, true)
                    .add("serie", StringType, true)
                    .add("subser", LongType, true)
                    .add("tpDoc", LongType, true)), true)), true)
                .add("xNome", StringType, true)), true))
            .add("infCarga", new StructType()
              .add("infQ", ArrayType(new StructType()
                .add("cUnid", LongType, true)
                .add("qCarga", DoubleType, true)
                .add("tpMed", StringType, true)), true)
              .add("proPred", StringType, true)
              .add("vCarga", DoubleType, true)
              .add("vCargaAverb", DoubleType, true)
              .add("xOutCat", StringType, true))
            .add("infCteSub", new StructType()
              .add("chCte", DoubleType, true)
              .add("indAlteraToma", LongType, true)
              .add("tomaICMS", new StructType()
                .add("refNFe", DoubleType, true)))
            .add("infDoc", new StructType()
              .add("infNF", ArrayType(new StructType()
                .add("PIN", LongType, true)
                .add("dEmi", DateType, true)
                .add("dPrev", DateType, true)
                .add("infUnidTransp", new StructType()
                  .add("idUnidTransp", StringType, true)
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidTransp", LongType, true))
                .add("mod", LongType, true)
                .add("nCFOP", LongType, true)
                .add("nDoc", StringType, true)
                .add("nPed", StringType, true)
                .add("nPeso", DoubleType, true)
                .add("nRoma", LongType, true)
                .add("serie", StringType, true)
                .add("vBC", DoubleType, true)
                .add("vBCST", DoubleType, true)
                .add("vICMS", DoubleType, true)
                .add("vNF", DoubleType, true)
                .add("vProd", DoubleType, true)
                .add("vST", DoubleType, true)), true)
              .add("infNFe", ArrayType(new StructType()
                .add("PIN", LongType, true)
                .add("chave", StringType, true)
                .add("dPrev", DateType, true)
                .add("infUnidCarga", new StructType()
                  .add("idUnidCarga", StringType, true)
                  .add("lacUnidCarga", ArrayType(new StructType()
                    .add("nLacre", StringType, true)), true)
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidCarga", LongType, true))
                .add("infUnidTransp", ArrayType(new StructType()
                  .add("idUnidTransp", StringType, true)
                  .add("infUnidCarga", new StructType()
                    .add("idUnidCarga", StringType, true)
                    .add("lacUnidCarga", new StructType()
                      .add("nLacre", StringType, true))
                    .add("qtdRat", DoubleType, true)
                    .add("tpUnidCarga", LongType, true))
                  .add("lacUnidTransp", ArrayType(new StructType()
                    .add("nLacre", StringType, true)), true)
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidTransp", LongType, true)), true)), true)
              .add("infOutros", ArrayType(new StructType()
                .add("dEmi", DateType, true)
                .add("dPrev", DateType, true)
                .add("descOutros", StringType, true)
                .add("infUnidCarga", new StructType()
                  .add("idUnidCarga", StringType, true)
                  .add("lacUnidCarga", new StructType()
                    .add("nLacre", StringType, true))
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidCarga", LongType, true))
                .add("infUnidTransp", new StructType()
                  .add("idUnidTransp", StringType, true)
                  .add("infUnidCarga", new StructType()
                    .add("idUnidCarga", StringType, true)
                    .add("lacUnidCarga", new StructType()
                      .add("nLacre", StringType, true))
                    .add("qtdRat", DoubleType, true)
                    .add("tpUnidCarga", LongType, true))
                  .add("lacUnidTransp", new StructType()
                    .add("nLacre", StringType, true))
                  .add("qtdRat", DoubleType, true)
                  .add("tpUnidTransp", LongType, true))
                .add("nDoc", StringType, true)
                .add("tpDoc", LongType, true)), true))
            .add("infGlobalizado", new StructType()
              .add("xObs", StringType, true))
            .add("infModal", new StructType()
              .add("_versaoModal", DoubleType, true)
              .add("aereo", new StructType()
                .add("dPrevAereo", DateType, true)
                .add("nMinu", LongType, true)
                .add("nOCA", LongType, true)
                .add("natCarga", new StructType()
                  .add("cInfManu", ArrayType(LongType, true), true)
                  .add("xDime", StringType, true))
                .add("peri", ArrayType(new StructType()
                  .add("infTotAP", new StructType()
                    .add("qTotProd", DoubleType, true)
                    .add("uniAP", LongType, true))
                  .add("nONU", LongType, true)
                  .add("qTotEmb", DoubleType, true)), true)
                .add("tarifa", new StructType()
                  .add("CL", StringType, true)
                  .add("cTar", StringType, true)
                  .add("vTar", DoubleType, true)))
              .add("aquav", new StructType()
                .add("balsa", ArrayType(new StructType()
                  .add("xBalsa", StringType, true)), true)
                .add("detCont", new StructType()
                  .add("nCont", LongType, true))
                .add("direc", StringType, true)
                .add("irin", StringType, true)
                .add("nViag", LongType, true)
                .add("tpNav", LongType, true)
                .add("vAFRMM", DoubleType, true)
                .add("vPrest", DoubleType, true)
                .add("xNavio", StringType, true))
              .add("duto", new StructType()
                .add("dFim", DateType, true)
                .add("dIni", DateType, true)
                .add("vTar", DoubleType, true))
              .add("ferrov", new StructType()
                .add("fluxo", StringType, true)
                .add("tpTraf", LongType, true)
                .add("trafMut", new StructType()
                  .add("ferrEmi", LongType, true)
                  .add("respFat", LongType, true)
                  .add("vFrete", DoubleType, true)))
              .add("multimodal", new StructType()
                .add("COTM", StringType, true)
                .add("indNegociavel", LongType, true)
                .add("seg", new StructType()
                  .add("infSeg", new StructType()
                    .add("CNPJ", LongType, true)
                    .add("xSeg", StringType, true))
                  .add("nApol", StringType, true)
                  .add("nAver", StringType, true)))
              .add("rodo", new StructType()
                .add("RNTRC", StringType, true)
                .add("occ", ArrayType(new StructType()
                  .add("dEmi", DateType, true)
                  .add("emiOcc", new StructType()
                    .add("CNPJ", LongType, true)
                    .add("IE", LongType, true)
                    .add("UF", StringType, true)
                    .add("cInt", StringType, true)
                    .add("fone", LongType, true))
                  .add("nOcc", LongType, true)
                  .add("serie", StringType, true)), true)))
            .add("infServVinc", new StructType()
              .add("infCTeMultimodal", ArrayType(new StructType()
                .add("chCTeMultimodal", DoubleType, true)), true)
              .add("veicNovos", ArrayType(new StructType()
                .add("cCor", StringType, true)
                .add("cMod", StringType, true)
                .add("chassi", StringType, true)
                .add("vFrete", DoubleType, true)
                .add("vUnit", DoubleType, true)
                .add("xCor", StringType, true)), true))
          )
          .add("infCteAnu", new StructType()
            .add("chCte", DoubleType, true)
            .add("dEmi", DateType, true))
          .add("infCteComp", ArrayType(new StructType()
            .add("chCTe", DoubleType, true)), true)
          .add("infRespTec", new StructType()
            .add("CNPJ", LongType, true)
            .add("email", StringType, true)
            .add("fone", LongType, true)
            .add("hashCSRT", StringType, true)
            .add("idCSRT", LongType, true)
            .add("xContato", StringType, true))
          .add("infSolicNFF", new StructType()
            .add("xSolic", StringType, true))
          .add("receb", new StructType()
            .add("CNPJ", LongType, true)
            .add("CPF", LongType, true)
            .add("IE", StringType, true)
            .add("email", StringType, true)
            .add("enderReceb", new StructType()
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
            .add("xNome", StringType, true))
          .add("rem", new StructType()
            .add("CNPJ", LongType, true)
            .add("CPF", LongType, true)
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
            .add("xNome", StringType, true))
          .add("vPrest", new StructType()
            .add("Comp", ArrayType(new StructType()
              .add("vComp", DoubleType, true)
              .add("xNome", StringType, true)), true)
            .add("vRec", DoubleType, true)
            .add("vTPrest", DoubleType, true))
        ))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val schema = createSchema()
    // Lista de anos com base nas variáveis externas
    val anoList = (anoInicio to anoFim).map(_.toString).toList

    anoList.foreach { ano =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/$tipoDocumento/$ano"

      // Registrar o horário de início da iteração
      val startTime = LocalDateTime.now()
      println(s"Início da iteração para $ano: $startTime")
      println(s"Lendo dados do caminho: $parquetPath")

      // 1. Carrega o arquivo Parquet
      val parquetDF = spark.read.parquet(parquetPath)

      // 2. Seleciona as colunas e filtra MODELO = 57
      val xmlDF = parquetDF
        .filter($"MODELO" === 57) // Filtra onde MODELO é igual a 64
        .select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSUSVD",
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
        $"parsed.protCTe.infProt._Id".as("infprot_Id"),
        $"parsed.protCTe.infProt.chCTe".as("chave"),
        $"parsed.protCTe.infProt.cStat".as("infprot_cstat"),
        $"parsed.protCTe.infProt.dhRecbto".as("infprot_dhrecbto"),
        $"parsed.protCTe.infProt.digVal".as("infprot_digVal"),
        $"parsed.protCTe.infProt.nProt".as("infprot_nProt"),
        $"parsed.protCTe.infProt.tpAmb".as("infprot_tpAmb"),
        $"parsed.protCTe.infProt.verAplic".as("infprot_verAplic"),
        $"parsed.protCTe.infProt.xMotivo".as("infprot_xMotivo"),
        $"parsed.CTe.infCTeSupl.qrCodCTe".as("infCTeSupl_qrCodCTe"),
        $"parsed.CTe.infCte._Id".as("infCte_Id"),
        $"parsed.CTe.infCte._versao".as("infCte_versao"),
        $"parsed.CTe.infCte.autXML".as("infCte_autXML"),
        $"parsed.CTe.infCte.compl.Entrega.comData.dProg".as("compl_Entrega_comData_dProg"),
        $"parsed.CTe.infCte.compl.Entrega.comData.tpPer".as("compl_Entrega_comData_tpPer"),
        $"parsed.CTe.infCte.compl.Entrega.comHora.hProg".as("compl_Entrega_comHora_hProg"),
        $"parsed.CTe.infCte.compl.Entrega.comHora.tpHor".as("compl_Entrega_comHora_tpHor"),
        $"parsed.CTe.infCte.compl.Entrega.noInter.hFim".as("compl_Entrega_noInter_hFim"),
        $"parsed.CTe.infCte.compl.Entrega.noInter.hIni".as("compl_Entrega_noInter_hIni"),
        $"parsed.CTe.infCte.compl.Entrega.noInter.tpHor".as("compl_Entrega_noInter_tpHor"),
        $"parsed.CTe.infCte.compl.Entrega.noPeriodo.dFim".as("compl_Entrega_noPeriodo_dFim"),
        $"parsed.CTe.infCte.compl.Entrega.noPeriodo.dIni".as("compl_Entrega_noPeriodo_dIni"),
        $"parsed.CTe.infCte.compl.Entrega.noPeriodo.tpPer".as("compl_Entrega_noPeriodo_tpPer"),
        $"parsed.CTe.infCte.compl.Entrega.semData.tpPer".as("compl_Entrega_semData_tpPer"),
        $"parsed.CTe.infCte.compl.Entrega.semHora.tpHor".as("compl_Entrega_semHora_tpHor"),
        $"parsed.CTe.infCte.compl.ObsCont".as("compl_ObsCont"),
        $"parsed.CTe.infCte.compl.ObsFisco".as("compl_ObsFisco"),
        $"parsed.CTe.infCte.compl.destCalc".as("compl_destCalc"),
        $"parsed.CTe.infCte.compl.fluxo.pass".as("compl_fluxo_pass"), // ArrayType
        $"parsed.CTe.infCte.compl.fluxo.xDest".as("compl_fluxo_xDest"),
        $"parsed.CTe.infCte.compl.fluxo.xOrig".as("compl_fluxo_xOrig"),
        $"parsed.CTe.infCte.compl.fluxo.xRota".as("compl_fluxo_xRota"),
        $"parsed.CTe.infCte.compl.origCalc".as("compl_origCalc"),
        $"parsed.CTe.infCte.compl.xCaracAd".as("compl_xCaracAd"),
        $"parsed.CTe.infCte.compl.xCaracSer".as("compl_xCaracSer"),
        $"parsed.CTe.infCte.compl.xEmi".as("compl_xEmi"),
        $"parsed.CTe.infCte.compl.xObs".as("compl_xObs"),
        $"parsed.CTe.infCte.dest.CNPJ".as("dest_cnpj"),
        $"parsed.CTe.infCte.dest.CPF".as("dest_cpf"),
        $"parsed.CTe.infCte.dest.IE".as("dest_ie"),
        $"parsed.CTe.infCte.dest.ISUF".as("dest_isuf"),
        $"parsed.CTe.infCte.dest.email".as("dest_email"),
        $"parsed.CTe.infCte.dest.enderDest.CEP".as("enderdest_cep"),
        $"parsed.CTe.infCte.dest.enderDest.UF".as("enderdest_uf"),
        $"parsed.CTe.infCte.dest.enderDest.cMun".as("enderdest_cmun"),
        $"parsed.CTe.infCte.dest.enderDest.cPais".as("enderdest_cpais"),
        $"parsed.CTe.infCte.dest.enderDest.nro".as("enderdest_nro"),
        $"parsed.CTe.infCte.dest.enderDest.xBairro".as("enderdest_xbairro"),
        $"parsed.CTe.infCte.dest.enderDest.xCpl".as("enderdest_xcpl"),
        $"parsed.CTe.infCte.dest.enderDest.xLgr".as("enderdest_xlgr"),
        $"parsed.CTe.infCte.dest.enderDest.xMun".as("enderdest_xmun"),
        $"parsed.CTe.infCte.dest.enderDest.xPais".as("enderdest_xpais"),
        $"parsed.CTe.infCte.dest.fone".as("dest_fone"),
        $"parsed.CTe.infCte.dest.xNome".as("dest_xnome"),
        $"parsed.CTe.infCte.emit.CNPJ".as("emit_cnpj"),
        $"parsed.CTe.infCte.emit.CPF".as("emit_cpf"),
        $"parsed.CTe.infCte.emit.CRT".as("emit_crt"),
        $"parsed.CTe.infCte.emit.IE".as("emit_ie"),
        $"parsed.CTe.infCte.emit.IEST".as("emit_iest"),
        $"parsed.CTe.infCte.emit.enderEmit.CEP".as("enderemit_cep"),
        $"parsed.CTe.infCte.emit.enderEmit.UF".as("enderemit_uf"),
        $"parsed.CTe.infCte.emit.enderEmit.cMun".as("enderemit_cmun"),
        $"parsed.CTe.infCte.emit.enderEmit.fone".as("enderemit_fone"),
        $"parsed.CTe.infCte.emit.enderEmit.nro".as("enderemit_nro"),
        $"parsed.CTe.infCte.emit.enderEmit.xBairro".as("enderemit_xbairro"),
        $"parsed.CTe.infCte.emit.enderEmit.xCpl".as("enderemit_xcpl"),
        $"parsed.CTe.infCte.emit.enderEmit.xLgr".as("enderemit_xlgr"),
        $"parsed.CTe.infCte.emit.enderEmit.xMun".as("enderemit_xmun"),
        $"parsed.CTe.infCte.emit.xFant".as("emit_xfant"),
        $"parsed.CTe.infCte.emit.xNome".as("emit_xnome"),
        $"parsed.CTe.infCte.exped.CNPJ".as("exped_cnpj"),
        $"parsed.CTe.infCte.exped.CPF".as("exped_cpf"),
        $"parsed.CTe.infCte.exped.IE".as("exped_ie"),
        $"parsed.CTe.infCte.exped.email".as("exped_email"),
        $"parsed.CTe.infCte.exped.fone".as("exped_fone"),
        $"parsed.CTe.infCte.exped.xNome".as("exped_xnome"),
        $"parsed.CTe.infCte.exped.enderExped.CEP".as("enderexped_cep"),
        $"parsed.CTe.infCte.exped.enderExped.UF".as("enderexped_uf"),
        $"parsed.CTe.infCte.exped.enderExped.cMun".as("enderexped_cmun"),
        $"parsed.CTe.infCte.exped.enderExped.cPais".as("enderexped_cpais"),
        $"parsed.CTe.infCte.exped.enderExped.nro".as("enderexped_nro"),
        $"parsed.CTe.infCte.exped.enderExped.xBairro".as("enderexped_xbairro"),
        $"parsed.CTe.infCte.exped.enderExped.xCpl".as("enderexped_xcpl"),
        $"parsed.CTe.infCte.exped.enderExped.xLgr".as("enderexped_xlgr"),
        $"parsed.CTe.infCte.exped.enderExped.xMun".as("enderexped_xmun"),
        $"parsed.CTe.infCte.exped.enderExped.xPais".as("enderexped_xpais"),
        $"parsed.CTe.infCte.ide.CFOP".as("ide_cfop"),
        $"parsed.CTe.infCte.ide.UFEnv".as("ide_ufenv"),
        $"parsed.CTe.infCte.ide.UFFim".as("ide_uffim"),
        $"parsed.CTe.infCte.ide.UFIni".as("ide_ufini"),
        $"parsed.CTe.infCte.ide.cCT".as("ide_cct"),
        $"parsed.CTe.infCte.ide.cDV".as("ide_cdv"),
        $"parsed.CTe.infCte.ide.cMunEnv".as("ide_cmunenv"),
        $"parsed.CTe.infCte.ide.cMunFim".as("ide_cmunfim"),
        $"parsed.CTe.infCte.ide.cMunIni".as("ide_cmunini"),
        $"parsed.CTe.infCte.ide.cUF".as("ide_cuf"),
        $"parsed.CTe.infCte.ide.dhCont".as("ide_dhcont"),
        $"parsed.CTe.infCte.ide.dhEmi".as("ide_dhemi"),
        $"parsed.CTe.infCte.ide.indGlobalizado".as("ide_indglobalizado"),
        $"parsed.CTe.infCte.ide.indIEToma".as("ide_indietoma"),
        $"parsed.CTe.infCte.ide.mod".as("ide_mod"),
        $"parsed.CTe.infCte.ide.modal".as("ide_modal"),
        $"parsed.CTe.infCte.ide.nCT".as("ide_nct"),
        $"parsed.CTe.infCte.ide.natOp".as("ide_natop"),
        $"parsed.CTe.infCte.ide.procEmi".as("ide_procemi"),
        $"parsed.CTe.infCte.ide.retira".as("ide_retira"),
        $"parsed.CTe.infCte.ide.serie".as("ide_serie"),
        $"parsed.CTe.infCte.ide.toma3.toma".as("ide_toma3_toma"),
        $"parsed.CTe.infCte.ide.toma4.CNPJ".as("ide_toma4_cnpj"),
        $"parsed.CTe.infCte.ide.toma4.CPF".as("ide_toma4_cpf"),
        $"parsed.CTe.infCte.ide.toma4.IE".as("ide_toma4_ie"),
        $"parsed.CTe.infCte.ide.toma4.email".as("ide_toma4_email"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.CEP".as("ide_toma4_endertoma_cep"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.UF".as("ide_toma4_endertoma_uf"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.cMun".as("ide_toma4_endertoma_cmun"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.cPais".as("ide_toma4_endertoma_cpais"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.nro".as("ide_toma4_endertoma_nro"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.xBairro".as("ide_toma4_endertoma_xbairro"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.xCpl".as("ide_toma4_endertoma_xcpl"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.xLgr".as("ide_toma4_endertoma_xlgr"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.xMun".as("ide_toma4_endertoma_xmun"),
        $"parsed.CTe.infCte.ide.toma4.enderToma.xPais".as("ide_toma4_endertoma_xpais"),
        $"parsed.CTe.infCte.ide.toma4.fone".as("ide_toma4_fone"),
        $"parsed.CTe.infCte.ide.toma4.toma".as("ide_toma4_toma"),
        $"parsed.CTe.infCte.ide.toma4.xFant".as("ide_toma4_xfant"),
        $"parsed.CTe.infCte.ide.toma4.xNome".as("ide_toma4_xnome"),
        $"parsed.CTe.infCte.ide.tpAmb".as("ide_tpamb"),
        $"parsed.CTe.infCte.ide.tpCTe".as("ide_tpcte"),
        $"parsed.CTe.infCte.ide.tpEmis".as("ide_tpemis"),
        $"parsed.CTe.infCte.ide.tpImp".as("ide_tpimp"),
        $"parsed.CTe.infCte.ide.tpServ".as("ide_tpserv"),
        $"parsed.CTe.infCte.ide.verProc".as("ide_verproc"),
        $"parsed.CTe.infCte.ide.xDetRetira".as("ide_xdetretira"),
        $"parsed.CTe.infCte.ide.xJust".as("ide_xjust"),
        $"parsed.CTe.infCte.ide.xMunEnv".as("ide_xmunenv"),
        $"parsed.CTe.infCte.ide.xMunFim".as("ide_xmunfim"),
        $"parsed.CTe.infCte.ide.xMunIni".as("ide_xmunini"),
        $"parsed.CTe.infCte.infCTeNorm.infDoc.infNF".as("infNF"),
        $"parsed.CTe.infCte.infCTeNorm.infDoc.infNFe".as("infNFe"),
        $"parsed.CTe.infCte.infCTeNorm.infDoc.infOutros".as("infOutros"),
        $"parsed.CTe.infCte.infCTeNorm.infGlobalizado.xObs".as("infGlobalizado_xObs"),
        $"parsed.CTe.infCte.infCTeNorm.infModal._versaoModal".as("infModal_versaoModal"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aereo.dPrevAereo".as("aereo_dPrevAereo"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aereo.nMinu".as("aereo_nMinu"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aereo.nOCA".as("aereo_nOCA"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aereo.natCarga".as("aereo_natCarga"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aereo.peri".as("aereo_peri"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aereo.tarifa".as("aereo_tarifa"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.balsa".as("aquav_balsa") ,
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.detCont.nCont".as("aquav_detCont_nCont"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.direc".as("aquav_direc"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.irin".as("aquav_irin"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.nViag".as("aquav_nViag"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.tpNav".as("aquav_tpNav"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.vAFRMM".as("aquav_vAFRMM"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.vPrest".as("aquav_vPrest"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.aquav.xNavio".as("aquav_xNavio"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.duto.dFim".as("duto_dFim"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.duto.dIni".as("duto_dIni"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.duto.vTar".as("duto_vTar"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.ferrov.fluxo".as("ferrov_fluxo"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.ferrov.tpTraf".as("ferrov_tpTraf"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.ferrov.trafMut.ferrEmi".as("ferrov_trafMut_ferrEmi"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.ferrov.trafMut.respFat".as("ferrov_trafMut_respFat"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.ferrov.trafMut.vFrete".as("ferrov_trafMut_vFrete"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.multimodal.COTM".as("multimodal_COTM"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.multimodal.indNegociavel".as("multimodal_indNegociavel"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.multimodal.seg.infSeg.CNPJ".as("seg_infSeg_CNPJ") ,
        $"parsed.CTe.infCte.infCTeNorm.infModal.multimodal.seg.infSeg.xSeg".as("seg_infSeg_xSeg"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.multimodal.seg.nApol".as("seg_nApol"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.multimodal.seg.nAver".as("seg_nAver"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.rodo.RNTRC".as("rodo_RNTRC"),
        $"parsed.CTe.infCte.infCTeNorm.infModal.rodo.occ".as("occ"),
        $"parsed.CTe.infCte.infCTeNorm.infServVinc.infCTeMultimodal".as("infServVinc_infCTeMultimodal"),
        $"parsed.CTe.infCte.infCTeNorm.infServVinc.veicNovos".as("infServVinc_veicNovos") ,
        $"parsed.CTe.infCte.infCteAnu.chCte".as("infCteAnu_chCte"),
        $"parsed.CTe.infCte.infCteAnu.dEmi".as("infCteAnu_dEmi"),
        $"parsed.CTe.infCte.infCteComp".as("infCteComp"),
        $"parsed.CTe.infCte.infRespTec.CNPJ".as("infRespTec_CNPJ"),
        $"parsed.CTe.infCte.infRespTec.email".as("infRespTec_email"),
        $"parsed.CTe.infCte.infRespTec.fone".as("infRespTec_fone"),
        $"parsed.CTe.infCte.infRespTec.hashCSRT".as("infRespTec_hashCSRT"),
        $"parsed.CTe.infCte.infRespTec.idCSRT".as("infRespTec_idCSRT"),
        $"parsed.CTe.infCte.infRespTec.xContato".as("infRespTec_xContato"),
        $"parsed.CTe.infCte.infSolicNFF.xSolic".as("infSolicNFF_xSolic"),
        $"parsed.CTe.infCte.receb.CNPJ".as("receb_CNPJ"),
        $"parsed.CTe.infCte.receb.CPF".as("receb_CPF"),
        $"parsed.CTe.infCte.receb.IE".as("receb_IE"),
        $"parsed.CTe.infCte.receb.email".as("receb_email"),
        $"parsed.CTe.infCte.receb.enderReceb.CEP".as("receb_enderReceb_CEP"),
        $"parsed.CTe.infCte.receb.enderReceb.UF".as("receb_enderReceb_UF"),
        $"parsed.CTe.infCte.receb.enderReceb.cMun".as("receb_enderReceb_cMun"),
        $"parsed.CTe.infCte.receb.enderReceb.cPais".as("receb_enderReceb_cPais"),
        $"parsed.CTe.infCte.receb.enderReceb.nro".as("receb_enderReceb_nro"),
        $"parsed.CTe.infCte.receb.enderReceb.xBairro".as("receb_enderReceb_xBairro"),
        $"parsed.CTe.infCte.receb.enderReceb.xCpl".as("receb_enderReceb_xCpl"),
        $"parsed.CTe.infCte.receb.enderReceb.xLgr".as("receb_enderReceb_xLgr"),
        $"parsed.CTe.infCte.receb.enderReceb.xMun".as("receb_enderReceb_xMun"),
        $"parsed.CTe.infCte.receb.enderReceb.xPais".as("receb_enderReceb_xPais"),
        $"parsed.CTe.infCte.receb.fone".as("receb_fone"),
        $"parsed.CTe.infCte.receb.xNome".as("receb_xNome"),
        $"parsed.CTe.infCte.rem.CNPJ".as("rem_CNPJ"),
        $"parsed.CTe.infCte.rem.CPF".as("rem_CPF"),
        $"parsed.CTe.infCte.rem.IE".as("rem_IE"),
        $"parsed.CTe.infCte.rem.email".as("rem_email"),
        $"parsed.CTe.infCte.rem.enderReme.CEP".as("rem_enderReme_CEP"),
        $"parsed.CTe.infCte.rem.enderReme.UF".as("rem_enderReme_UF"),
        $"parsed.CTe.infCte.rem.enderReme.cMun".as("rem_enderReme_cMun"),
        $"parsed.CTe.infCte.rem.enderReme.cPais".as("rem_enderReme_cPais"),
        $"parsed.CTe.infCte.rem.enderReme.nro".as("rem_enderReme_nro"),
        $"parsed.CTe.infCte.rem.enderReme.xBairro".as("rem_enderReme_xBairro"),
        $"parsed.CTe.infCte.rem.enderReme.xCpl".as("rem_enderReme_xCpl"),
        $"parsed.CTe.infCte.rem.enderReme.xLgr".as("rem_enderReme_xLgr"),
        $"parsed.CTe.infCte.rem.enderReme.xMun".as("rem_enderReme_xMun"),
        $"parsed.CTe.infCte.rem.enderReme.xPais".as("rem_enderReme_xPais"),
        $"parsed.CTe.infCte.rem.fone".as("rem_fone"),
        $"parsed.CTe.infCte.rem.xFant".as("rem_xFant"),
        $"parsed.CTe.infCte.rem.xNome".as("rem_xNome"),
        $"parsed.CTe.infCte.vPrest.Comp".as("vPrest_Comp"),
        $"parsed.CTe.infCte.vPrest.vRec".as("vPrest_vRec"),
        $"parsed.CTe.infCte.vPrest.vTPrest".as("vPrest_vTPrest")
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
      val repartitionedDF = selectedDFComParticao.repartition(40)

      // Escrever os dados particionados
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
        .partitionBy("chave_particao") // Garante a separação por partição
        .save("/datalake/prata/sources/dbms/dec/cte/CTe")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")
    }
  }
}

//CTeLegadoProcessor.main(Array())
