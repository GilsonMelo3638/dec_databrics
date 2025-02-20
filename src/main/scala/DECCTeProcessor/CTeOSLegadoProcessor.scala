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

object CTeOSLegadoProcessor {
    // Variáveis externas para o intervalo de meses e ano de processamento
    val anoInicio = 2025
    val anoFim = 2025
    val tipoDocumento = "cte"

    // Função para criar o esquema de forma modular

  import org.apache.spark.sql.types._

  def createSchema(): StructType = {
    new StructType()
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
          .add("cStat", StringType, true)
          .add("chCTe", StringType, true)
          .add("dhRecbto", StringType, true)
          .add("digVal", StringType, true)
          .add("nProt", StringType, true)
          .add("tpAmb", StringType, true)
          .add("verAplic", StringType, true)
          .add("xMotivo", StringType, true)))
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

        // 2. Seleciona as colunas e filtra MODELO = 67
        val xmlDF = parquetDF
          .filter($"MODELO" === 67) // Filtra onde MODELO é igual a 64
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
          $"parsed.protCTe.infProt._Id".as("infprot_id"),
          $"parsed.protCTe.infProt.cStat".as("protcte_cstat"),
          $"parsed.protCTe.infProt.chCTe".as("chave"),
          $"parsed.protCTe.infProt.dhRecbto".as("infprot_dhrecbto"),
          $"parsed.protCTe.infProt.digVal".as("infprot_digval"),
          $"parsed.protCTe.infProt.nProt".as("infprot_nprot"),
          $"parsed.protCTe.infProt.tpAmb".as("infprot_tpamb"),
          $"parsed.protCTe.infProt.xMotivo".as("infprot_xmotivo"),
          $"parsed.protCTe.infProt.verAplic".as("infprot_veraplic"),
          $"parsed.CTeOS.infCte.emit.CNPJ".as("emit_cnpj"),
          $"parsed.CTeOS.infCte.emit.CRT".as("emit_crt"),
          $"parsed.CTeOS.infCte.emit.IE".as("emit_ie"),
          $"parsed.CTeOS.infCte.emit.enderEmit.CEP".as("enderemit_cep"),
          $"parsed.CTeOS.infCte.emit.enderEmit.UF".as("enderemit_uf"),
          $"parsed.CTeOS.infCte.emit.enderEmit.cMun".as("enderemit_cmun"),
          $"parsed.CTeOS.infCte.emit.enderEmit.fone".as("enderemit_fone"),
          $"parsed.CTeOS.infCte.emit.enderEmit.nro".as("enderemit_nro"),
          $"parsed.CTeOS.infCte.emit.enderEmit.xBairro".as("enderemit_xbairro"),
          $"parsed.CTeOS.infCte.emit.enderEmit.xCpl".as("enderemit_xcpl"),
          $"parsed.CTeOS.infCte.emit.enderEmit.xLgr".as("enderemit_xlgr"),
          $"parsed.CTeOS.infCte.emit.enderEmit.xMun".as("enderemit_xmun"),
          $"parsed.CTeOS.infCte.emit.xFant".as("emit_xfant"),
          $"parsed.CTeOS.infCte.emit.xNome".as("emit_xnome"),
          $"parsed.CTeOS.infCte.ide.CFOP".as("ide_cfop"),
          $"parsed.CTeOS.infCte.ide.UFEnv".as("ide_ufenv"),
          $"parsed.CTeOS.infCte.ide.UFFim".as("ide_uffim"),
          $"parsed.CTeOS.infCte.ide.UFIni".as("ide_ufini"),
          $"parsed.CTeOS.infCte.ide.cCT".as("ide_cct"),
          $"parsed.CTeOS.infCte.ide.cDV".as("ide_cdv"),
          $"parsed.CTeOS.infCte.ide.cMunEnv".as("ide_cmunenv"),
          $"parsed.CTeOS.infCte.ide.cMunFim".as("ide_cmunfim"),
          $"parsed.CTeOS.infCte.ide.cMunIni".as("ide_cmunini"),
          $"parsed.CTeOS.infCte.ide.cUF".as("ide_cuf"),
          $"parsed.CTeOS.infCte.ide.dhEmi".as("ide_dhemi"),
          $"parsed.CTeOS.infCte.ide.indIEToma".as("ide_indietoma"),
          $"parsed.CTeOS.infCte.ide.mod".as("ide_mod"),
          $"parsed.CTeOS.infCte.ide.modal".as("ide_modal"),
          $"parsed.CTeOS.infCte.ide.nCT".as("ide_nct"),
          $"parsed.CTeOS.infCte.ide.natOp".as("ide_natop"),
          $"parsed.CTeOS.infCte.ide.procEmi".as("ide_procemi"),
          $"parsed.CTeOS.infCte.ide.serie".as("ide_serie"),
          $"parsed.CTeOS.infCte.ide.tpAmb".as("ide_tpamb"),
          $"parsed.CTeOS.infCte.ide.tpCTe".as("ide_tpcte"),
          $"parsed.CTeOS.infCte.ide.tpEmis".as("ide_tpemis"),
          $"parsed.CTeOS.infCte.ide.tpImp".as("ide_tpimp"),
          $"parsed.CTeOS.infCte.ide.tpServ".as("ide_tpserv"),
          $"parsed.CTeOS.infCte.ide.verProc".as("ide_verproc"),
          $"parsed.CTeOS.infCte.ide.xMunEnv".as("ide_xmunenv"),
          $"parsed.CTeOS.infCte.ide.xMunFim".as("ide_xmunfim"),
          $"parsed.CTeOS.infCte.ide.xMunIni".as("ide_xmunini"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.CST".as("icms00_cst"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.pICMS".as("icms00_picms"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.vBC".as("icms00_vbc"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS00.vICMS".as("icms00_vicms"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.CST".as("icms20_cst"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.pICMS".as("icms20_picms"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.vBC".as("icms20_vbc"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS20.vICMS".as("icms20_vicms"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS45.CST".as("icms45_cst"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.CST".as("icms90_cst"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.pICMS".as("icms90_picms"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.vBC".as("icms90_vbc"),
          $"parsed.CTeOS.infCte.imp.ICMS.ICMS90.vICMS".as("icms90_vicms"),
          $"parsed.CTeOS.infCte.imp.ICMSUFFim.vICMSUFFim".as("icmsuffim_vicmsuffim"),
          $"parsed.CTeOS.infCte.imp.vTotTrib".as("imp_vtottrib"),
          $"parsed.CTeOS.infCte.infCTeNorm.cobr.fat.nFat".as("fat_nfat"),
          $"parsed.CTeOS.infCte.infCTeNorm.cobr.fat.vLiq".as("fat_vliq"),
          $"parsed.CTeOS.infCte.infCTeNorm.infDocRef".as("infDocRef"),
          $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.placa".as("veic_placa"),
          $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.CNPJ".as("veic_prop_cnpj"),
          $"parsed.CTeOS.infCte.infCTeNorm.infModal.rodoOS.veic.prop.xNome".as("veic_prop_xnome"),
          $"parsed.CTeOS.infCte.infCTeNorm.infServico.infQ.qCarga".as("infq_qcarga"),
          $"parsed.CTeOS.infCte.infCTeNorm.infServico.xDescServ".as("infserv_xdescserv"),
          $"parsed.CTeOS.infCte.infRespTec.CNPJ".as("infresptec_cnpj"),
          $"parsed.CTeOS.infCte.infRespTec.email".as("infresptec_email"),
          $"parsed.CTeOS.infCte.infRespTec.fone".as("infresptec_fone"),
          $"parsed.CTeOS.infCte.infRespTec.xContato".as("infresptec_xcontato"),
          $"parsed.CTeOS.infCte.toma.CNPJ".as("toma_cnpj"),
          $"parsed.CTeOS.infCte.toma.CPF".as("toma_cpf"),
          $"parsed.CTeOS.infCte.toma.IE".as("toma_ie"),
          $"parsed.CTeOS.infCte.toma.enderToma.CEP".as("toma_cep"),
          $"parsed.CTeOS.infCte.toma.enderToma.UF".as("toma_uf"),
          $"parsed.CTeOS.infCte.toma.enderToma.cMun".as("toma_cmun"),
          $"parsed.CTeOS.infCte.toma.enderToma.nro".as("toma_nro"),
          $"parsed.CTeOS.infCte.toma.enderToma.xBairro".as("toma_xbairro"),
          $"parsed.CTeOS.infCte.toma.enderToma.xLgr".as("toma_xlgr"),
          $"parsed.CTeOS.infCte.toma.enderToma.xMun".as("toma_xmun"),
          $"parsed.CTeOS.infCte.toma.xNome".as("toma_xnome"),
          $"parsed.CTeOS.infCte.vPrest.vTPrest".as("vprest_vtprest")
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
          .save("/datalake/prata/sources/dbms/dec/cte/CTeOS")

        // Registrar o horário de término da gravação
        val saveEndTime = LocalDateTime.now()
        println(s"Gravação concluída: $saveEndTime")    }
    }
  }

//CTeOSLegadoProcessor.main(Array())
