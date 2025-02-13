//scp "C:\dec-nfe-det\target\NFeDetPrata-0.0.1-SNAPSHOT.jar"  gamelo@10.69.22.71:src/main/scala/NFeDetPrata-0.0.1-SNAPSHOT.jar
//hdfs dfs -put -f /export/home/gamelo/src/main/scala/NFeDetPrata-0.0.1-SNAPSHOT.jar /app/dec
//hdfs dfs -ls /app/dec
//hdfs dfs -rm -skipTrash /app/dec/NFeDetPrata-0.0.1-SNAPSHOT.jar
// spark-submit \
//  --class DECJNFeDetJob.NFeDetProcessor \
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
//  hdfs://sepladbigdata/app/dec/NFeDetPrata-0.0.1-SNAPSHOT.jar
package DECNFCeDetJob

import com.databricks.spark.xml.functions.from_xml
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
object NFCeDetProcessor {
  def main(args: Array[String]): Unit = {
    val tipoDocumento = "nfce"
      val spark = SparkSession.builder()
        .appName("ExtractInfNFe")
        .config("spark.sql.broadcastTimeout", "600") // Timeout para operações de broadcast
        .config("spark.executor.memory", "16g") // Memória do executor
        .config("spark.driver.memory", "8g") // Memória do driver
        .config("spark.executor.memoryOverhead", "4096") // Overhead de memória do executor
        .config("spark.yarn.executor.memoryOverhead", "4096") // Overhead de memória no YARN
        .config("spark.network.timeout", "600s") // Tempo de timeout da rede
        .config("spark.executor.heartbeatInterval", "30s") // Evita perda de executores
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") // Desabilita broadcast automático
        .config("spark.sql.shuffle.partitions", "800") // Aumenta partições no shuffle
        .config("spark.default.parallelism", "800") // Melhora paralelismo
        .config("spark.shuffle.service.enabled", "true") // Ativa serviço de shuffle
        .config("spark.shuffle.file.buffer", "1m") // Buffer maior para reduzir I/O
        .config("spark.reducer.maxSizeInFlight", "96m") // Reduz pressão no shuffle
        .config("spark.memory.fraction", "0.6") // Ajusta a fração de memória para execução
        .config("spark.memory.storageFraction", "0.5") // Ajusta fração de memória para armazenamento
        .config("spark.dynamicAllocation.enabled", "true") // Ativa alocação dinâmica
        .config("spark.dynamicAllocation.minExecutors", "10") // Mínimo de executores
        .config("spark.dynamicAllocation.maxExecutors", "40") // Máximo de executores
        .config("spark.dynamicAllocation.initialExecutors", "20") // Melhor distribuição inicial
        .config("spark.sql.hive.filesourcePartitionFileCacheSize", "524288000") // Cache de partições Hive
        .config("spark.storage.replication", "2") // Mantém duas réplicas
        .enableHiveSupport() // Ativa suporte ao Hive
        .getOrCreate()

    import spark.implicits._
    // Definindo intervalo de dias: diasAntesInicio (10 dias atrás) até diasAntesFim (ontem)
    val diasAntesInicio = LocalDate.now.minusDays(15)
    val diasAntesFim = LocalDate.now.minusDays(1)

    // Formatação para ano, mês e dia
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    // Iterando pelas datas no intervalo
    (0 to diasAntesInicio.until(diasAntesFim).getDays).foreach { dayOffset =>
      val currentDate = diasAntesInicio.plusDays(dayOffset)

      val ano = currentDate.getYear
      val mes = f"${currentDate.getMonthValue}%02d"
      val dia = f"${currentDate.getDayOfMonth}%02d"
      val anoMesDia = s"$ano$mes$dia"

      val parquetPath = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processar_det/$anoMesDia"
      val parquetPathProcessado = s"/datalake/bronze/sources/dbms/dec/processamento/$tipoDocumento/processado/$anoMesDia"
      val destino = s"/datalake/prata/sources/dbms/dec/$tipoDocumento/det/"

      println(s"Processando para: Ano: $ano, Mês: $mes, Dia: $dia")
      println(s"Caminho de origem: $parquetPath")
      println(s"Caminho de destino: $parquetPathProcessado")

      // Verificar se o diretório existe antes de processar
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val parquetPathExists = fs.exists(new Path(parquetPath))

      if (parquetPathExists) {
        val parquetDF = spark.read.parquet(parquetPath)

        // Verificação de quantidade total e distinta
        val totalCount = parquetDF.count()
        val distinctCount = parquetDF.select("chave").distinct().count()

        if (totalCount != distinctCount) {
          println(s"Erro: Total de registros ($totalCount) é diferente do total de registros distintos ($distinctCount) no caminho: $parquetPath")
          throw new IllegalStateException("Inconsistência nos dados: total e distinto não coincidem.")
        } else {
          println(s"Verificação bem-sucedida: Total ($totalCount) e distintos ($distinctCount) são iguais no caminho: $parquetPath")
        }

        // 2. Selecionar a coluna que contém o XML (ex: "XML_DOCUMENTO_CLOB")
        val xmlDF = parquetDF.select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSU",
          $"DHPROC",
          $"EMITENTE",
          $"DESTINATARIO",
          $"UF_EMITENTE",
          $"UF_DESTINATARIO",
          $"DHEMI",
          $"IP_TRANSMISSOR"
        )
        // 3. Usar `from_xml` para ler o XML da coluna usando o esquema definido
        val schema = createSchema() // Assumindo que a função `createSchema` foi declarada
        val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))

        // 4. Selecionar os campos desejados
        val selectedDF = parsedDF.select(
          $"NSU",
          $"DHPROC",
          $"EMITENTE",
          $"DESTINATARIO",
          $"UF_EMITENTE",
          $"UF_DESTINATARIO",
          $"DHEMI",
          $"IP_TRANSMISSOR",
          date_format(to_timestamp($"DHPROC", "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH").as("DHPROC_FORMATADO"),
          $"parsed.protNFe.infProt.chNFe".as("chave"),
          $"parsed.protNFe.infProt.cStat".as("infprot_cstat"),
          $"parsed.protNFe.infProt.dhRecbto".as("infprot_dhrecbto"),
          explode($"parsed.NFe.infNFe.det").as("det") // Explode para descompactar os itens de `det`
        )
          .select(
            $"NSU",
            $"IP_TRANSMISSOR",
            $"DHPROC_FORMATADO",
            $"EMITENTE",
            $"DESTINATARIO",
            $"UF_EMITENTE",
            $"UF_DESTINATARIO",
            $"DHEMI",
            $"chave",
            $"infprot_cstat",
            $"infprot_dhrecbto",
            $"det._nItem".as("nItem"), // Corrigido para acessar o atributo `_nItem`
            $"det.prod.CFOP".as("CFOP"),
            $"det.prod.CEST".as("CEST"),
            $"det.prod.CNPJFab".as("CNPJFab"),
            $"det.prod.NCM".as("NCM"),
            $"det.prod.EXTIPI".as("EXTIPI"),
            $"det.prod.cBarra".as("cBarra"),
            $"det.prod.cBarraTrib".as("cBarraTrib"),
            $"det.prod.cBenef".as("cBenef"),
            $"det.prod.cEAN".as("cEAN"),
            $"det.prod.cEANTrib".as("cEANTrib"),
            $"det.prod.cProd".as("cProd"),
            $"det.prod.indEscala".as("indEscala"),
            $"det.prod.indTot".as("indTot"),
            $"det.prod.nFCI".as("nFCI"),
            $"det.prod.nItemPed".as("nItemPed"),
            $"det.prod.qCom".as("qCom"),
            $"det.prod.qTrib".as("qTrib"),
            $"det.prod.uCom".as("uCom"),
            $"det.prod.uTrib".as("uTrib"),
            $"det.prod.vDesc".as("vDesc"),
            $"det.prod.vFrete".as("vFrete"),
            $"det.prod.vOutro".as("vOutro"),
            $"det.prod.vProd".as("vProd"),
            $"det.prod.vSeg".as("vSeg"),
            $"det.prod.vUnCom".as("vUnCom"),
            $"det.prod.vUnTrib".as("vUnTrib"),
            $"det.prod.xPed".as("xPed"),
            $"det.prod.xProd".as("xProd"),
            $"det.prod.DI.CNPJ".as("DI_CNPJ"),
            $"det.prod.DI.UFDesemb".as("DI_UFDesemb"),
            $"det.prod.DI.UFTerceiro".as("DI_UFTerceiro"),
            $"det.prod.DI.adi.cFabricante".as("DI_adi_cFabricante"),
            $"det.prod.DI.adi.nAdicao".as("DI_adi_nAdicao"),
            $"det.prod.DI.adi.nDraw".as("DI_adi_nDraw"),
            $"det.prod.DI.adi.nSeqAdic".as("DI_adi_nSeqAdic"),
            $"det.prod.DI.cExportador".as("DI_cExportador"),
            $"det.prod.DI.dDI".as("DI_dDI"),
            $"det.prod.DI.dDesemb".as("DI_dDesemb"),
            $"det.prod.DI.nDI".as("DI_nDI"),
            $"det.prod.DI.tpIntermedio".as("DI_tpIntermedio"),
            $"det.prod.DI.tpViaTransp".as("DI_tpViaTransp"),
            $"det.prod.DI.vAFRMM".as("DI_vAFRMM"),
            $"det.prod.DI.xLocDesemb".as("DI_xLocDesemb"),
            $"det.prod.gCred.cCredPresumido".as("gCred_cCredPresumido"),
            $"det.prod.gCred.pCredPresumido".as("gCred_pCredPresumido"),
            $"det.prod.gCred.vCredPresumido".as("gCred_vCredPresumido"),
            $"det.prod.infProdNFF.cOperNFF".as("infProdNFF_cOperNFF"),
            $"det.prod.infProdNFF.cProdFisco".as("infProdNFF_cProdFisco"),
            $"det.prod.med.cProdANVISA".as("med_cProdANVISA"),
            $"det.prod.med.vPMC".as("med_vPMC"),
            $"det.prod.med.xMotivoIsencao".as("med_xMotivoIsencao"),
            $"det.prod.arma".as("arma"),
            $"det.prod.veicProd.CMT".as("veicProd_CMT"),
            $"det.prod.veicProd.VIN".as("veicProd_VIN"),
            $"det.prod.veicProd.anoFab".as("veicProd_anoFab"),
            $"det.prod.veicProd.anoMod".as("veicProd_anoMod"),
            $"det.prod.veicProd.cCor".as("veicProd_cCor"),
            $"det.prod.veicProd.cCorDENATRAN".as("veicProd_cCorDENATRAN"),
            $"det.prod.veicProd.cMod".as("veicProd_cMod"),
            $"det.prod.veicProd.chassi".as("veicProd_chassi"),
            $"det.prod.veicProd.cilin".as("veicProd_cilin"),
            $"det.prod.veicProd.condVeic".as("veicProd_condVeic"),
            $"det.prod.veicProd.dist".as("veicProd_dist"),
            $"det.prod.veicProd.espVeic".as("veicProd_espVeic"),
            $"det.prod.veicProd.lota".as("veicProd_lota"),
            $"det.prod.veicProd.nMotor".as("veicProd_nMotor"),
            $"det.prod.veicProd.nSerie".as("veicProd_nSerie"),
            $"det.prod.veicProd.pesoB".as("veicProd_pesoB"),
            $"det.prod.veicProd.pesoL".as("veicProd_pesoL"),
            $"det.prod.veicProd.pot".as("veicProd_pot"),
            $"det.prod.veicProd.tpComb".as("veicProd_tpComb"),
            $"det.prod.veicProd.tpOp".as("veicProd_tpOp"),
            $"det.prod.veicProd.tpPint".as("veicProd_tpPint"),
            $"det.prod.veicProd.tpRest".as("veicProd_tpRest"),
            $"det.prod.veicProd.tpVeic".as("veicProd_tpVeic"),
            $"det.prod.veicProd.xCor".as("veicProd_xCor"),
            $"det.prod.NVE".as("NVE"),
            $"det.prod.detExport.exportInd.chNFe".as("detExport_exportInd_chNFe"),
            $"det.prod.detExport.exportInd.nRE".as("detExport_exportInd_nRE"),
            $"det.prod.detExport.exportInd.qExport".as("detExport_exportInd_qExport"),
            $"det.prod.detExport.nDraw".as("detExport_nDraw"),
            $"det.prod.comb.CIDE.qBCProd".as("comb_CIDE_qBCProd"),
            $"det.prod.comb.CIDE.vAliqProd".as("comb_CIDE_vAliqProd"),
            $"det.prod.comb.CIDE.vCIDE".as("comb_CIDE_vCIDE"),
            $"det.prod.comb.CODIF".as("comb_CODIF"),
            $"det.prod.comb.UFCons".as("comb_UFCons"),
            $"det.prod.comb.cProdANP".as("comb_cProdANP"),
            $"det.prod.comb.descANP".as("comb_descANP"),
            $"det.prod.comb.encerrante.nBico".as("comb_encerrante_nBico"),
            $"det.prod.comb.encerrante.nBomba".as("comb_encerrante_nBomba"),
            $"det.prod.comb.encerrante.nTanque".as("comb_encerrante_nTanque"),
            $"det.prod.comb.encerrante.vEncFin".as("comb_encerrante_vEncFin"),
            $"det.prod.comb.encerrante.vEncIni".as("comb_encerrante_vEncIni"),
            $"det.prod.comb.origComb.cUFOrig".as("comb_origComb_cUFOrig"),
            $"det.prod.comb.origComb.indImport".as("comb_origComb_indImport"),
            $"det.prod.comb.origComb.pOrig".as("comb_origComb_pOrig"),
            $"det.prod.comb.pBio".as("comb_pBio"),
            $"det.prod.comb.pGLP".as("comb_pGLP"),
            $"det.prod.comb.pGNi".as("comb_pGNi"),
            $"det.prod.comb.pGNn".as("comb_pGNn"),
            $"det.prod.comb.qTemp".as("comb_qTemp"),
            $"det.prod.comb.vPart".as("comb_vPart"),
            $"det.prod.rastro.cAgreg".as("rastro_cAgreg"),
            $"det.prod.rastro.dFab".as("rastro_dFab"),
            $"det.prod.rastro.dVal".as("rastro_dVal"),
            $"det.prod.rastro.nLote".as("rastro_nLote"),
            $"det.prod.rastro.qLote".as("rastro_qLote"),
            $"det.imposto.ICMS.ICMS00.orig".as("ICMS00_orig"),
            $"det.imposto.ICMS.ICMS00.CST".as("ICMS00_CST"),
            $"det.imposto.ICMS.ICMS00.modBC".as("ICMS00_modBC"),
            $"det.imposto.ICMS.ICMS00.pICMS".as("ICMS00_pICMS"),
            $"det.imposto.ICMS.ICMS00.vBC".as("ICMS00_vBC"),
            $"det.imposto.ICMS.ICMS00.vICMS".as("ICMS00_vICMS"),
            $"det.imposto.ICMS.ICMS00.pFCP".as("ICMS00_pFCP"),
            $"det.imposto.ICMS.ICMS00.vFCP".as("ICMS00_vFCP"),
            $"det.imposto.ICMS.ICMS02.orig".as("ICMS02_orig"),
            $"det.imposto.ICMS.ICMS02.CST".as("ICMS02_CST"),
            $"det.imposto.ICMS.ICMS02.adRemICMS".as("ICMS02_adRemICMS"),
            $"det.imposto.ICMS.ICMS02.qBCMono".as("ICMS02_qBCMono"),
            $"det.imposto.ICMS.ICMS02.vICMSMono".as("ICMS02_vICMSMono"),
            $"det.imposto.ICMS.ICMS10.orig".as("ICMS10_orig"),
            $"det.imposto.ICMS.ICMS10.CST".as("ICMS10_CST"),
            $"det.imposto.ICMS.ICMS10.modBC".as("ICMS10_modBC"),
            $"det.imposto.ICMS.ICMS10.modBCST".as("ICMS10_modBCST"),
            $"det.imposto.ICMS.ICMS10.pICMS".as("ICMS10_pICMS"),
            $"det.imposto.ICMS.ICMS10.vBC".as("ICMS10_vBC"),
            $"det.imposto.ICMS.ICMS10.vICMS".as("ICMS10_vICMS"),
            $"det.imposto.ICMS.ICMS10.pFCP".as("ICMS10_pFCP"),
            $"det.imposto.ICMS.ICMS10.vFCP".as("ICMS10_vFCP"),
            $"det.imposto.ICMS.ICMS10.vICMSST".as("ICMS10_vICMSST"),
            $"det.imposto.ICMS.ICMS10.vBCST".as("ICMS10_vBCST"),
            $"det.imposto.ICMS.ICMS10.vFCPST".as("ICMS10_vFCPST"),
            $"det.imposto.ICMS.ICMS10.pMVAST".as("ICMS10_pMVAST"),
            $"det.imposto.ICMS.ICMS10.vICMSSTDeson".as("ICMS10_vICMSSTDeson"),
            $"det.imposto.ICMS.ICMS10.motDesICMSST".as("ICMS10_motDesICMSST"),
            $"det.imposto.ICMS.ICMS10.pFCPST".as("ICMS10_pFCPST"),
            $"det.imposto.ICMS.ICMS10.pICMSST".as("ICMS10_pICMSST"),
            $"det.imposto.ICMS.ICMS10.pRedBCST".as("ICMS10_pRedBCST"),
            $"det.imposto.ICMS.ICMS10.vBCFCP".as("ICMS10_vBCFCP"),
            $"det.imposto.ICMS.ICMS10.vBCFCPST".as("ICMS10_vBCFCPST"),
            $"det.imposto.ICMS.ICMS15.CST".as("ICMS15_CST"),
            $"det.imposto.ICMS.ICMS15.adRemICMS".as("ICMS15_adRemICMS"),
            $"det.imposto.ICMS.ICMS15.adRemICMSReten".as("ICMS15_adRemICMSReten"),
            $"det.imposto.ICMS.ICMS15.orig".as("ICMS15_orig"),
            $"det.imposto.ICMS.ICMS15.qBCMono".as("ICMS15_qBCMono"),
            $"det.imposto.ICMS.ICMS15.qBCMonoReten".as("ICMS15_qBCMonoReten"),
            $"det.imposto.ICMS.ICMS15.vICMSMono".as("ICMS15_vICMSMono"),
            $"det.imposto.ICMS.ICMS15.vICMSMonoReten".as("ICMS15_vICMSMonoReten"),
            $"det.imposto.ICMS.ICMS20.CST".as("ICMS20_CST"),
            $"det.imposto.ICMS.ICMS20.indDeduzDeson".as("ICMS20_indDeduzDeson"),
            $"det.imposto.ICMS.ICMS20.modBC".as("ICMS20_modBC"),
            $"det.imposto.ICMS.ICMS20.motDesICMS".as("ICMS20_motDesICMS"),
            $"det.imposto.ICMS.ICMS20.orig".as("ICMS20_orig"),
            $"det.imposto.ICMS.ICMS20.pFCP".as("ICMS20_pFCP"),
            $"det.imposto.ICMS.ICMS20.pICMS".as("ICMS20_pICMS"),
            $"det.imposto.ICMS.ICMS20.pRedBC".as("ICMS20_pRedBC"),
            $"det.imposto.ICMS.ICMS20.vBC".as("ICMS20_vBC"),
            $"det.imposto.ICMS.ICMS20.vBCFCP".as("ICMS20_vBCFCP"),
            $"det.imposto.ICMS.ICMS20.vFCP".as("ICMS20_vFCP"),
            $"det.imposto.ICMS.ICMS20.vICMS".as("ICMS20_vICMS"),
            $"det.imposto.ICMS.ICMS20.vICMSDeson".as("ICMS20_vICMSDeson"),
            $"det.imposto.ICMS.ICMS30.CST".as("ICMS30_CST"),
            $"det.imposto.ICMS.ICMS30.indDeduzDeson".as("ICMS30_indDeduzDeson"),
            $"det.imposto.ICMS.ICMS30.modBCST".as("ICMS30_modBCST"),
            $"det.imposto.ICMS.ICMS30.motDesICMS".as("ICMS30_motDesICMS"),
            $"det.imposto.ICMS.ICMS30.orig".as("ICMS30_orig"),
            $"det.imposto.ICMS.ICMS30.pFCPST".as("ICMS30_pFCPST"),
            $"det.imposto.ICMS.ICMS30.pICMSST".as("ICMS30_pICMSST"),
            $"det.imposto.ICMS.ICMS30.pMVAST".as("ICMS30_pMVAST"),
            $"det.imposto.ICMS.ICMS30.pRedBCST".as("ICMS30_pRedBCST"),
            $"det.imposto.ICMS.ICMS30.vBCFCPST".as("ICMS30_vBCFCPST"),
            $"det.imposto.ICMS.ICMS30.vBCST".as("ICMS30_vBCST"),
            $"det.imposto.ICMS.ICMS30.vFCPST".as("ICMS30_vFCPST"),
            $"det.imposto.ICMS.ICMS30.vICMSDeson".as("ICMS30_vICMSDeson"),
            $"det.imposto.ICMS.ICMS30.vICMSST".as("ICMS30_vICMSST"),
            $"det.imposto.ICMS.ICMS40.CST".as("ICMS40_CST"),
            $"det.imposto.ICMS.ICMS40.indDeduzDeson".as("ICMS40_indDeduzDeson"),
            $"det.imposto.ICMS.ICMS40.motDesICMS".as("ICMS40_motDesICMS"),
            $"det.imposto.ICMS.ICMS40.orig".as("ICMS40_orig"),
            $"det.imposto.ICMS.ICMS40.vICMSDeson".as("ICMS40_vICMSDeson"),
            $"det.imposto.ICMS.ICMS51.CST".as("ICMS51_CST"),
            $"det.imposto.ICMS.ICMS51.modBC".as("ICMS51_modBC"),
            $"det.imposto.ICMS.ICMS51.orig".as("ICMS51_orig"),
            $"det.imposto.ICMS.ICMS51.pDif".as("ICMS51_pDif"),
            $"det.imposto.ICMS.ICMS51.pICMS".as("ICMS51_pICMS"),
            $"det.imposto.ICMS.ICMS51.pRedBC".as("ICMS51_pRedBC"),
            $"det.imposto.ICMS.ICMS51.vBC".as("ICMS51_vBC"),
            $"det.imposto.ICMS.ICMS51.vICMS".as("ICMS51_vICMS"),
            $"det.imposto.ICMS.ICMS51.vICMSDif".as("ICMS51_vICMSDif"),
            $"det.imposto.ICMS.ICMS51.vICMSOp".as("ICMS51_vICMSOp"),
            $"det.imposto.ICMS.ICMS53.CST".as("ICMS53_CST"),
            $"det.imposto.ICMS.ICMS53.adRemICMS".as("ICMS53_adRemICMS"),
            $"det.imposto.ICMS.ICMS53.orig".as("ICMS53_orig"),
            $"det.imposto.ICMS.ICMS53.pDif".as("ICMS53_pDif"),
            $"det.imposto.ICMS.ICMS53.qBCMono".as("ICMS53_qBCMono"),
            $"det.imposto.ICMS.ICMS53.vICMSMono".as("ICMS53_vICMSMono"),
            $"det.imposto.ICMS.ICMS53.vICMSMonoDif".as("ICMS53_vICMSMonoDif"),
            $"det.imposto.ICMS.ICMS53.vICMSMonoOp".as("ICMS53_vICMSMonoOp"),
            $"det.imposto.ICMS.ICMS60.CST".as("ICMS60_CST"),
            $"det.imposto.ICMS.ICMS60.orig".as("ICMS60_orig"),
            $"det.imposto.ICMS.ICMS60.pFCPSTRet".as("ICMS60_pFCPSTRet"),
            $"det.imposto.ICMS.ICMS60.pICMSEfet".as("ICMS60_pICMSEfet"),
            $"det.imposto.ICMS.ICMS60.pRedBCEfet".as("ICMS60_pRedBCEfet"),
            $"det.imposto.ICMS.ICMS60.pST".as("ICMS60_pST"),
            $"det.imposto.ICMS.ICMS60.vBCEfet".as("ICMS60_vBCEfet"),
            $"det.imposto.ICMS.ICMS60.vBCFCPSTRet".as("ICMS60_vBCFCPSTRet"),
            $"det.imposto.ICMS.ICMS60.vBCSTRet".as("ICMS60_vBCSTRet"),
            $"det.imposto.ICMS.ICMS60.vFCPSTRet".as("ICMS60_vFCPSTRet"),
            $"det.imposto.ICMS.ICMS60.vICMSEfet".as("ICMS60_vICMSEfet"),
            $"det.imposto.ICMS.ICMS60.vICMSSTRet".as("ICMS60_vICMSSTRet"),
            $"det.imposto.ICMS.ICMS60.vICMSSubstituto".as("ICMS60_vICMSSubstituto"),
            $"det.imposto.ICMS.ICMS61.CST".as("ICMS61_CST"),
            $"det.imposto.ICMS.ICMS61.adRemICMSRet".as("ICMS61_adRemICMSRet"),
            $"det.imposto.ICMS.ICMS61.orig".as("ICMS61_orig"),
            $"det.imposto.ICMS.ICMS61.qBCMonoRet".as("ICMS61_qBCMonoRet"),
            $"det.imposto.ICMS.ICMS61.vICMSMonoRet".as("ICMS61_vICMSMonoRet"),
            $"det.imposto.ICMS.ICMS70.CST".as("ICMS70_CST"),
            $"det.imposto.ICMS.ICMS70.indDeduzDeson".as("ICMS70_indDeduzDeson"),
            $"det.imposto.ICMS.ICMS70.modBC".as("ICMS70_modBC"),
            $"det.imposto.ICMS.ICMS70.modBCST".as("ICMS70_modBCST"),
            $"det.imposto.ICMS.ICMS70.motDesICMS".as("ICMS70_motDesICMS"),
            $"det.imposto.ICMS.ICMS70.motDesICMSST".as("ICMS70_motDesICMSST"),
            $"det.imposto.ICMS.ICMS70.orig".as("ICMS70_orig"),
            $"det.imposto.ICMS.ICMS70.pICMS".as("ICMS70_pICMS"),
            $"det.imposto.ICMS.ICMS70.pICMSST".as("ICMS70_pICMSST"),
            $"det.imposto.ICMS.ICMS70.pMVAST".as("ICMS70_pMVAST"),
            $"det.imposto.ICMS.ICMS70.pRedBC".as("ICMS70_pRedBC"),
            $"det.imposto.ICMS.ICMS70.pRedBCST".as("ICMS70_pRedBCST"),
            $"det.imposto.ICMS.ICMS70.vBC".as("ICMS70_vBC"),
            $"det.imposto.ICMS.ICMS70.vBCST".as("ICMS70_vBCST"),
            $"det.imposto.ICMS.ICMS70.vICMS".as("ICMS70_vICMS"),
            $"det.imposto.ICMS.ICMS70.vICMSDeson".as("ICMS70_vICMSDeson"),
            $"det.imposto.ICMS.ICMS70.vICMSST".as("ICMS70_vICMSST"),
            $"det.imposto.ICMS.ICMS70.vICMSSTDeson".as("ICMS70_vICMSSTDeson"),
            $"det.imposto.ICMS.ICMS90.CST".as("ICMS90_CST"),
            $"det.imposto.ICMS.ICMS90.indDeduzDeson".as("ICMS90_indDeduzDeson"),
            $"det.imposto.ICMS.ICMS90.modBC".as("ICMS90_modBC"),
            $"det.imposto.ICMS.ICMS90.modBCST".as("ICMS90_modBCST"),
            $"det.imposto.ICMS.ICMS90.motDesICMS".as("ICMS90_motDesICMS"),
            $"det.imposto.ICMS.ICMS90.orig".as("ICMS90_orig"),
            $"det.imposto.ICMS.ICMS90.pFCP".as("ICMS90_pFCP"),
            $"det.imposto.ICMS.ICMS90.pICMS".as("ICMS90_pICMS"),
            $"det.imposto.ICMS.ICMS90.pICMSST".as("ICMS90_pICMSST"),
            $"det.imposto.ICMS.ICMS90.pMVAST".as("ICMS90_pMVAST"),
            $"det.imposto.ICMS.ICMS90.pRedBC".as("ICMS90_pRedBC"),
            $"det.imposto.ICMS.ICMS90.pRedBCST".as("ICMS90_pRedBCST"),
            $"det.imposto.ICMS.ICMS90.vBC".as("ICMS90_vBC"),
            $"det.imposto.ICMS.ICMS90.vBCFCP".as("ICMS90_vBCFCP"),
            $"det.imposto.ICMS.ICMS90.vBCST".as("ICMS90_vBCST"),
            $"det.imposto.ICMS.ICMS90.vFCP".as("ICMS90_vFCP"),
            $"det.imposto.ICMS.ICMS90.vICMS".as("ICMS90_vICMS"),
            $"det.imposto.ICMS.ICMS90.vICMSDeson".as("ICMS90_vICMSDeson"),
            $"det.imposto.ICMS.ICMS90.vICMSST".as("ICMS90_vICMSST"),
            $"det.imposto.ICMS.ICMSPart.CST".as("ICMSPart_CST"),
            $"det.imposto.ICMS.ICMSPart.UFST".as("ICMSPart_UFST"),
            $"det.imposto.ICMS.ICMSPart.modBC".as("ICMSPart_modBC"),
            $"det.imposto.ICMS.ICMSPart.modBCST".as("ICMSPart_modBCST"),
            $"det.imposto.ICMS.ICMSPart.orig".as("ICMSPart_orig"),
            $"det.imposto.ICMS.ICMSPart.pBCOp".as("ICMSPart_pBCOp"),
            $"det.imposto.ICMS.ICMSPart.pFCPST".as("ICMSPart_pFCPST"),
            $"det.imposto.ICMS.ICMSPart.pICMS".as("ICMSPart_pICMS"),
            $"det.imposto.ICMS.ICMSPart.pICMSST".as("ICMSPart_pICMSST"),
            $"det.imposto.ICMS.ICMSPart.pRedBC".as("ICMSPart_pRedBC"),
            $"det.imposto.ICMS.ICMSPart.pRedBCST".as("ICMSPart_pRedBCST"),
            $"det.imposto.ICMS.ICMSPart.vBC".as("ICMSPart_vBC"),
            $"det.imposto.ICMS.ICMSPart.vBCFCPST".as("ICMSPart_vBCFCPST"),
            $"det.imposto.ICMS.ICMSPart.vBCST".as("ICMSPart_vBCST"),
            $"det.imposto.ICMS.ICMSPart.vFCPST".as("ICMSPart_vFCPST"),
            $"det.imposto.ICMS.ICMSPart.vICMS".as("ICMSPart_vICMS"),
            $"det.imposto.ICMS.ICMSPart.vICMSST".as("ICMSPart_vICMSST"),
            $"det.imposto.ICMS.ICMSSN101.CSOSN".as("ICMSSN101_CSOSN"),
            $"det.imposto.ICMS.ICMSSN101.orig".as("ICMSSN101_orig"),
            $"det.imposto.ICMS.ICMSSN101.pCredSN".as("ICMSSN101_pCredSN"),
            $"det.imposto.ICMS.ICMSSN101.vCredICMSSN".as("ICMSSN101_vCredICMSSN"),
            $"det.imposto.ICMS.ICMSSN102.CSOSN".as("ICMSSN102_CSOSN"),
            $"det.imposto.ICMS.ICMSSN102.orig".as("ICMSSN102_orig"),
            $"det.imposto.ICMS.ICMSSN201.CSOSN".as("ICMSSN201_CSOSN"),
            $"det.imposto.ICMS.ICMSSN201.modBCST".as("ICMSSN201_modBCST"),
            $"det.imposto.ICMS.ICMSSN201.orig".as("ICMSSN201_orig"),
            $"det.imposto.ICMS.ICMSSN201.pCredSN".as("ICMSSN201_pCredSN"),
            $"det.imposto.ICMS.ICMSSN201.pFCPST".as("ICMSSN201_pFCPST"),
            $"det.imposto.ICMS.ICMSSN201.pICMSST".as("ICMSSN201_pICMSST"),
            $"det.imposto.ICMS.ICMSSN201.pMVAST".as("ICMSSN201_pMVAST"),
            $"det.imposto.ICMS.ICMSSN201.pRedBCST".as("ICMSSN201_pRedBCST"),
            $"det.imposto.ICMS.ICMSSN201.vBCFCPST".as("ICMSSN201_vBCFCPST"),
            $"det.imposto.ICMS.ICMSSN201.vBCST".as("ICMSSN201_vBCST"),
            $"det.imposto.ICMS.ICMSSN201.vCredICMSSN".as("ICMSSN201_vCredICMSSN"),
            $"det.imposto.ICMS.ICMSSN201.vFCPST".as("ICMSSN201_vFCPST"),
            $"det.imposto.ICMS.ICMSSN201.vICMSST".as("ICMSSN201_vICMSST"),
            $"det.imposto.ICMS.ICMSSN202.CSOSN".as("ICMSSN202_CSOSN"),
            $"det.imposto.ICMS.ICMSSN202.modBCST".as("ICMSSN202_modBCST"),
            $"det.imposto.ICMS.ICMSSN202.orig".as("ICMSSN202_orig"),
            $"det.imposto.ICMS.ICMSSN202.pFCPST".as("ICMSSN202_pFCPST"),
            $"det.imposto.ICMS.ICMSSN202.pICMSST".as("ICMSSN202_pICMSST"),
            $"det.imposto.ICMS.ICMSSN202.pMVAST".as("ICMSSN202_pMVAST"),
            $"det.imposto.ICMS.ICMSSN202.pRedBCST".as("ICMSSN202_pRedBCST"),
            $"det.imposto.ICMS.ICMSSN202.vBCFCPST".as("ICMSSN202_vBCFCPST"),
            $"det.imposto.ICMS.ICMSSN202.vBCST".as("ICMSSN202_vBCST"),
            $"det.imposto.ICMS.ICMSSN202.vFCPST".as("ICMSSN202_vFCPST"),
            $"det.imposto.ICMS.ICMSSN202.vICMSST".as("ICMSSN202_vICMSST"),
            $"det.imposto.PIS.PISAliq.CST".as("PIS_PISAliq_CST"),
            $"det.imposto.PIS.PISAliq.vBC".as("PIS_PISAliq_vBC"),
            $"det.imposto.PIS.PISAliq.pPIS".as("PIS_PISAliq_pPIS"),
            $"det.imposto.PIS.PISAliq.vPIS".as("PIS_PISAliq_vPIS"),
            $"det.imposto.PIS.PISNT.CST".as("PIS_PISNT_CST"),
            $"det.imposto.PIS.PISOutr.CST".as("PIS_PISOutr_CST"),
            $"det.imposto.PIS.PISOutr.vBC".as("PIS_PISOutr_vBC"),
            $"det.imposto.PIS.PISOutr.pPIS".as("PIS_PISOutr_pPIS"),
            $"det.imposto.PIS.PISOutr.qBCProd".as("PIS_PISOutr_qBCProd"),
            $"det.imposto.PIS.PISOutr.vAliqProd".as("PIS_PISOutr_vAliqProd"),
            $"det.imposto.PIS.PISOutr.vPIS".as("PIS_PISOutr_vPIS"),
            $"det.imposto.PIS.PISQtde.CST".as("PIS_PISQtde_CST"),
            $"det.imposto.PIS.PISQtde.qBCProd".as("PIS_PISQtde_qBCProd"),
            $"det.imposto.PIS.PISQtde.vAliqProd".as("PIS_PISQtde_vAliqProd"),
            $"det.imposto.PIS.PISQtde.vPIS".as("PIS_PISQtde_vPIS"),
            $"det.imposto.PIS.PISST.indSomaPISST".as("PIS_PISST_indSomaPISST"),
            $"det.imposto.PIS.PISST.vBC".as("PIS_PISST_vBC"),
            $"det.imposto.PIS.PISST.pPIS".as("PIS_PISST_pPIS"),
            $"det.imposto.PIS.PISST.qBCProd".as("PIS_PISST_qBCProd"),
            $"det.imposto.PIS.PISST.vAliqProd".as("PIS_PISST_vAliqProd"),
            $"det.imposto.PIS.PISST.vPIS".as("PIS_PISST_vPIS"),
            $"det.imposto.PIS.vTotTrib".as("PIS_vTotTrib"),
            $"det.imposto.COFINS.COFINSAliq.CST".as("COFINSAliq_CST"),
            $"det.imposto.COFINS.COFINSAliq.pCOFINS".as("COFINSAliq_pCOFINS"),
            $"det.imposto.COFINS.COFINSAliq.vBC".as("COFINSAliq_vBC"),
            $"det.imposto.COFINS.COFINSAliq.vCOFINS".as("COFINSAliq_vCOFINS"),
            $"det.imposto.COFINS.COFINSNT.CST".as("COFINSNT_CST"),
            $"det.imposto.COFINS.COFINSOutr.CST".as("COFINSOutr_CST"),
            $"det.imposto.COFINS.COFINSOutr.pCOFINS".as("COFINSOutr_pCOFINS"),
            $"det.imposto.COFINS.COFINSOutr.qBCProd".as("COFINSOutr_qBCProd"),
            $"det.imposto.COFINS.COFINSOutr.vAliqProd".as("COFINSOutr_vAliqProd"),
            $"det.imposto.COFINS.COFINSOutr.vBC".as("COFINSOutr_vBC"),
            $"det.imposto.COFINS.COFINSOutr.vCOFINS".as("COFINSOutr_vCOFINS"),
            $"det.imposto.COFINS.COFINSQtde.CST".as("COFINSQtde_CST"),
            $"det.imposto.COFINS.COFINSQtde.qBCProd".as("COFINSQtde_qBCProd"),
            $"det.imposto.COFINS.COFINSQtde.vAliqProd".as("COFINSQtde_vAliqProd"),
            $"det.imposto.COFINS.COFINSQtde.vCOFINS".as("COFINSQtde_vCOFINS"),
            $"det.imposto.COFINS.COFINSST.indSomaCOFINSST".as("COFINSST_indSomaCOFINSST"),
            $"det.imposto.COFINS.COFINSST.pCOFINS".as("COFINSST_pCOFINS"),
            $"det.imposto.COFINS.COFINSST.qBCProd".as("COFINSST_qBCProd"),
            $"det.imposto.COFINS.COFINSST.vAliqProd".as("COFINSST_vAliqProd"),
            $"det.imposto.COFINS.COFINSST.vBC".as("COFINSST_vBC"),
            $"det.imposto.COFINS.COFINSST.vCOFINS".as("COFINSST_vCOFINS"),
            $"det.imposto.ICMS.ICMSSN500.CSOSN".as("ICMSSN500_CSOSN"),
            $"det.imposto.ICMS.ICMSSN500.orig".as("ICMSSN500_orig"),
            $"det.imposto.ICMS.ICMSSN500.pFCPSTRet".as("ICMSSN500_pFCPSTRet"),
            $"det.imposto.ICMS.ICMSSN500.pICMSEfet".as("ICMSSN500_pICMSEfet"),
            $"det.imposto.ICMS.ICMSSN500.pRedBCEfet".as("ICMSSN500_pRedBCEfet"),
            $"det.imposto.ICMS.ICMSSN500.pST".as("ICMSSN500_pST"),
            $"det.imposto.ICMS.ICMSSN500.vBCEfet".as("ICMSSN500_vBCEfet"),
            $"det.imposto.ICMS.ICMSSN500.vBCFCPSTRet".as("ICMSSN500_vBCFCPSTRet"),
            $"det.imposto.ICMS.ICMSSN500.vBCSTRet".as("ICMSSN500_vBCSTRet"),
            $"det.imposto.ICMS.ICMSSN500.vFCPSTRet".as("ICMSSN500_vFCPSTRet"),
            $"det.imposto.ICMS.ICMSSN500.vICMSEfet".as("ICMSSN500_vICMSEfet"),
            $"det.imposto.ICMS.ICMSSN500.vICMSSTRet".as("ICMSSN500_vICMSSTRet"),
            $"det.imposto.ICMS.ICMSSN500.vICMSSubstituto".as("ICMSSN500_vICMSSubstituto"),
            $"det.imposto.ICMS.ICMSSN900.CSOSN".as("ICMSSN900_CSOSN"),
            $"det.imposto.ICMS.ICMSSN900.modBC".as("ICMSSN900_modBC"),
            $"det.imposto.ICMS.ICMSSN900.modBCST".as("ICMSSN900_modBCST"),
            $"det.imposto.ICMS.ICMSSN900.orig".as("ICMSSN900_orig"),
            $"det.imposto.ICMS.ICMSSN900.pCredSN".as("ICMSSN900_pCredSN"),
            $"det.imposto.ICMS.ICMSSN900.pICMS".as("ICMSSN900_pICMS"),
            $"det.imposto.ICMS.ICMSSN900.pICMSST".as("ICMSSN900_pICMSST"),
            $"det.imposto.ICMS.ICMSSN900.pMVAST".as("ICMSSN900_pMVAST"),
            $"det.imposto.ICMS.ICMSSN900.pRedBC".as("ICMSSN900_pRedBC"),
            $"det.imposto.ICMS.ICMSSN900.pRedBCST".as("ICMSSN900_pRedBCST"),
            $"det.imposto.ICMS.ICMSSN900.vBC".as("ICMSSN900_vBC"),
            $"det.imposto.ICMS.ICMSSN900.vBCST".as("ICMSSN900_vBCST"),
            $"det.imposto.ICMS.ICMSSN900.vCredICMSSN".as("ICMSSN900_vCredICMSSN"),
            $"det.imposto.ICMS.ICMSSN900.vICMS".as("ICMSSN900_vICMS"),
            $"det.imposto.ICMS.ICMSSN900.vICMSST".as("ICMSSN900_vICMSST"),
            $"det.imposto.ICMS.ICMSST.CST".as("ICMSST_CST"),
            $"det.imposto.ICMS.ICMSST.orig".as("ICMSST_orig"),
            $"det.imposto.ICMS.ICMSST.pFCPSTRet".as("ICMSST_pFCPSTRet"),
            $"det.imposto.ICMS.ICMSST.pICMSEfet".as("ICMSST_pICMSEfet"),
            $"det.imposto.ICMS.ICMSST.pRedBCEfet".as("ICMSST_pRedBCEfet"),
            $"det.imposto.ICMS.ICMSST.pST".as("ICMSST_pST"),
            $"det.imposto.ICMS.ICMSST.vBCEfet".as("ICMSST_vBCEfet"),
            $"det.imposto.ICMS.ICMSST.vBCFCPSTRet".as("ICMSST_vBCFCPSTRet"),
            $"det.imposto.ICMS.ICMSST.vBCSTDest".as("ICMSST_vBCSTDest"),
            $"det.imposto.ICMS.ICMSST.vBCSTRet".as("ICMSST_vBCSTRet"),
            $"det.imposto.ICMS.ICMSST.vFCPSTRet".as("ICMSST_vFCPSTRet"),
            $"det.imposto.ICMS.ICMSST.vICMSEfet".as("ICMSST_vICMSEfet"),
            $"det.imposto.ICMS.ICMSST.vICMSSTDest".as("ICMSST_vICMSSTDest"),
            $"det.imposto.ICMS.ICMSST.vICMSSTRet".as("ICMSST_vICMSSTRet"),
            $"det.imposto.ICMS.ICMSST.vICMSSubstituto".as("ICMSST_vICMSSubstituto"),
            // Adicione os campos de ICMSUFDest
            $"det.imposto.ICMSUFDest.pFCPUFDest".as("ICMSUFDest_pFCPUFDest"),
            $"det.imposto.ICMSUFDest.pICMSInter".as("ICMSUFDest_pICMSInter"),
            $"det.imposto.ICMSUFDest.pICMSInterPart".as("ICMSUFDest_pICMSInterPart"),
            $"det.imposto.ICMSUFDest.pICMSUFDest".as("ICMSUFDest_pICMSUFDest"),
            $"det.imposto.ICMSUFDest.vBCFCPUFDest".as("ICMSUFDest_vBCFCPUFDest"),
            $"det.imposto.ICMSUFDest.vBCUFDest".as("ICMSUFDest_vBCUFDest"),
            $"det.imposto.ICMSUFDest.vFCPUFDest".as("ICMSUFDest_vFCPUFDest"),
            $"det.imposto.ICMSUFDest.vICMSUFDest".as("ICMSUFDest_vICMSUFDest"),
            $"det.imposto.ICMSUFDest.vICMSUFRemet".as("ICMSUFDest_vICMSUFRemet"),
            // Adicione os campos de II
            $"det.imposto.II.vBC".as("II_vBC"),
            $"det.imposto.II.vDespAdu".as("II_vDespAdu"),
            $"det.imposto.II.vII".as("II_vII"),
            $"det.imposto.II.vIOF".as("II_vIOF"),
            // Adicione os campos de IPI
            $"det.imposto.IPI.CNPJProd".as("IPI_CNPJProd"),
            $"det.imposto.IPI.IPINT.CST".as("IPINT_CST"),
            $"det.imposto.IPI.IPITrib.CST".as("IPITrib_CST"),
            $"det.imposto.IPI.IPITrib.pIPI".as("IPITrib_pIPI"),
            $"det.imposto.IPI.IPITrib.qUnid".as("IPITrib_qUnid"),
            $"det.imposto.IPI.IPITrib.vBC".as("IPITrib_vBC"),
            $"det.imposto.IPI.IPITrib.vIPI".as("IPITrib_vIPI"),
            $"det.imposto.IPI.IPITrib.vUnid".as("IPITrib_vUnid"),
            $"det.imposto.IPI.cEnq".as("IPI_cEnq"),
            $"det.imposto.IPI.cSelo".as("IPI_cSelo"),
            $"det.imposto.IPI.qSelo".as("IPI_qSelo"),
            $"det.imposto.ISSQN.cListServ".as("ISSQN_cListServ"),
            $"det.imposto.ISSQN.cMun".as("ISSQN_cMun"),
            $"det.imposto.ISSQN.cMunFG".as("ISSQN_cMunFG"),
            $"det.imposto.ISSQN.cPais".as("ISSQN_cPais"),
            $"det.imposto.ISSQN.cServico".as("ISSQN_cServico"),
            $"det.imposto.ISSQN.indISS".as("ISSQN_indISS"),
            $"det.imposto.ISSQN.indIncentivo".as("ISSQN_indIncentivo"),
            $"det.imposto.ISSQN.vAliq".as("ISSQN_vAliq"),
            $"det.imposto.ISSQN.vBC".as("ISSQN_vBC"),
            $"det.imposto.ISSQN.vISSQN".as("ISSQN_vISSQN"),
            $"det.impostoDevol.IPI.vIPIDevol".as("impostoDevol_IPI_vIPIDevol"),
            $"det.impostoDevol.pDevol".as("impostoDevol_pDevol"),
            $"det.infAdProd".as("infAdProd"),
            $"det.obsItem.obsCont._xCampo".as("obsItem_obsCont_xCampo"),
            $"det.obsItem.obsCont.xTexto".as("obsItem_obsCont_xTexto"),
            $"det.obsItem.obsFisco._xCampo".as("obsItem_obsFisco_xCampo"),
            $"det.obsItem.obsFisco.xTexto".as("obsItem_obsFisco_xTexto")
          )

          val selectedDFComParticao = selectedDF.withColumn("chave_particao", substring(col("chave"), 3, 4))

          // Obtendo as variações únicas de 'chave_particao' e coletando para uma lista
          val chaveParticoesUnicas = selectedDFComParticao
            .select("chave_particao")
            .distinct()
            .as[String]
            .collect()

          // Criando uma função para processar cada partição separadamente
          def processarParticao(chaveParticao: String): Unit = {
              val caminhoParticao = s"$destino/chave_particao=$chaveParticao"

              val particaoExiste = try {
                  val particaoDF = spark.read.parquet(caminhoParticao).select("chave")
                  !particaoDF.isEmpty
              } catch {
                  case _: Exception => false
              }
              val dfFiltrado = if (particaoExiste) {
                  println(s"[INFO] Partição $chaveParticao já existe. Filtrando novas chaves...")
                  val particaoDF = spark.read.parquet(caminhoParticao).select("chave").distinct()
                  selectedDFComParticao
                    .filter(col("chave_particao") === chaveParticao)
                    .join(particaoDF, Seq("chave"), "left_anti")
              } else {
                  println(s"[INFO] Partição $chaveParticao não existe. Criando nova partição...")
                  selectedDFComParticao.filter(col("chave_particao") === chaveParticao)
              }

//              val qtdRegistros = dfFiltrado.count()
//              println(s"[INFO] Partição $chaveParticao - Quantidade de registros a serem salvos: $qtdRegistros")

              dfFiltrado.write
                .mode("append")
                .format("parquet")
                .option("compression", "lz4")
                .option("parquet.block.size", 500 * 1024 * 1024)
                .save(caminhoParticao)

              println(s"[INFO] Finalizado processamento da partição: $chaveParticao")
          }
          // Iterando sobre cada partição e processando
          chaveParticoesUnicas.foreach(processarParticao)
        println(s"Gravação concluída para $anoMesDia")

        // Mover os arquivos para a pasta processada
        val srcPath = new Path(parquetPath)
        if (fs.exists(srcPath)) {
          val destPath = new Path(parquetPathProcessado)
          if (!fs.exists(destPath)) {
            fs.mkdirs(destPath)
          }
          fs.listStatus(srcPath).foreach { fileStatus =>
            val srcFile = fileStatus.getPath
            val destFile = new Path(destPath, srcFile.getName)
            fs.rename(srcFile, destFile)
          }
          fs.delete(srcPath, true)
          println(s"Arquivos movidos de $parquetPath para $parquetPathProcessado com sucesso.")
        }
      } else {
        println(s"Diretório de origem $parquetPath não encontrado.")
      }
    }
  }

  def createSchema(): StructType = {
    new StructType()
      .add("protNFe", new StructType()
        .add("infProt", new StructType()
          .add("chNFe", StringType, nullable = true)
          .add("cStat", StringType, nullable = true)
          .add("dhRecbto", StringType, nullable = true)
        )
      )
      .add("NFe", new StructType()
        .add("infNFe", new StructType()
          .add("ide", new StructType()
            .add("dhEmi", StringType, nullable = true)
            .add("cUF", StringType, nullable = true)
            .add("natOp", StringType, nullable = true)
          )
          .add("emit", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
            .add("enderEmit", new StructType()
              .add("xLgr", StringType, nullable = true)
              .add("nro", StringType, nullable = true)
              .add("xBairro", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
            )
          )
          .add("dest", new StructType()
            .add("CNPJ", StringType, nullable = true)
            .add("xNome", StringType, nullable = true)
            .add("enderDest", new StructType()
              .add("xLgr", StringType, nullable = true)
              .add("nro", StringType, nullable = true)
              .add("xBairro", StringType, nullable = true)
              .add("UF", StringType, nullable = true)
            )
          )
          .add("det", ArrayType(new StructType()
            .add("_nItem", StringType, nullable = true)
            .add("prod", new StructType()
              .add("CFOP", StringType, nullable = true)
              .add("CEST", StringType, nullable = true)
              .add("CNPJFab", StringType, nullable = true)
              .add("NCM", StringType, nullable = true)
              .add("EXTIPI", StringType, nullable = true)
              .add("cBarra", StringType, nullable = true)
              .add("cBarraTrib", StringType, nullable = true)
              .add("cBenef", StringType, nullable = true)
              .add("cEAN", StringType, nullable = true)
              .add("cEANTrib", StringType, nullable = true)
              .add("cProd", StringType, nullable = true)
              .add("indEscala", StringType, nullable = true)
              .add("indTot", StringType, nullable = true)
              .add("nFCI", StringType, nullable = true)
              .add("nItemPed", StringType, nullable = true)
              .add("qCom", DoubleType, nullable = true)
              .add("qTrib", DoubleType, nullable = true)
              .add("uCom", StringType, nullable = true)
              .add("uTrib", StringType, nullable = true)
              .add("vDesc", DoubleType, nullable = true)
              .add("vFrete", DoubleType, nullable = true)
              .add("vOutro", DoubleType, nullable = true)
              .add("vProd", DoubleType, nullable = true)
              .add("vSeg", DoubleType, nullable = true)
              .add("vUnCom", DoubleType, nullable = true)
              .add("vUnTrib", DoubleType, nullable = true)
              .add("xPed", StringType, nullable = true)
              .add("xProd", StringType, nullable = true)
              .add("DI", new StructType()
                .add("CNPJ", StringType, nullable = true)
                .add("UFDesemb", StringType, nullable = true)
                .add("UFTerceiro", StringType, nullable = true)
                .add("adi", new StructType()
                  .add("cFabricante", StringType, nullable = true)
                  .add("nAdicao", StringType, nullable = true)
                  .add("nDraw", StringType, nullable = true)
                  .add("nSeqAdic", StringType, nullable = true)
                )
                .add("cExportador", StringType, nullable = true)
                .add("dDI", StringType, nullable = true)
                .add("dDesemb", StringType, nullable = true)
                .add("nDI", StringType, nullable = true)
                .add("tpIntermedio", StringType, nullable = true)
                .add("tpViaTransp", StringType, nullable = true)
                .add("vAFRMM", DoubleType, nullable = true)
                .add("xLocDesemb", StringType, nullable = true)
              )
              .add("gCred", new StructType()
                .add("cCredPresumido", StringType, nullable = true)
                .add("pCredPresumido", DoubleType, nullable = true)
                .add("vCredPresumido", DoubleType, nullable = true)
              )
              .add("infProdNFF", new StructType()
                .add("cOperNFF", StringType, nullable = true)
                .add("cProdFisco", StringType, nullable = true)
              )
              .add("med", new StructType()
                .add("cProdANVISA", StringType, nullable = true)
                .add("vPMC", DoubleType, nullable = true)
                .add("xMotivoIsencao", StringType, nullable = true)
              )
              .add("arma", ArrayType(new StructType()
                .add("descr", StringType, nullable = true)
                .add("nCano", StringType, nullable = true)
                .add("nSerie", StringType, nullable = true)
                .add("tpArma", StringType, nullable = true)
              ))
              .add("veicProd", new StructType()
                .add("CMT", StringType, nullable = true)
                .add("VIN", StringType, nullable = true)
                .add("anoFab", StringType, nullable = true)
                .add("anoMod", StringType, nullable = true)
                .add("cCor", StringType, nullable = true)
                .add("cCorDENATRAN", StringType, nullable = true)
                .add("cMod", StringType, nullable = true)
                .add("chassi", StringType, nullable = true)
                .add("cilin", StringType, nullable = true)
                .add("condVeic", StringType, nullable = true)
                .add("dist", StringType, nullable = true)
                .add("espVeic", StringType, nullable = true)
                .add("lota", StringType, nullable = true)
                .add("nMotor", StringType, nullable = true)
                .add("nSerie", StringType, nullable = true)
                .add("pesoB", StringType, nullable = true)
                .add("pesoL", StringType, nullable = true)
                .add("pot", StringType, nullable = true)
                .add("tpComb", StringType, nullable = true)
                .add("tpOp", StringType, nullable = true)
                .add("tpPint", StringType, nullable = true)
                .add("tpRest", StringType, nullable = true)
                .add("tpVeic", StringType, nullable = true)
                .add("xCor", StringType, nullable = true)
              )
              .add("NVE", ArrayType(StringType))
              .add("detExport", ArrayType(new StructType()
                .add("exportInd", new StructType()
                  .add("chNFe", DoubleType, nullable = true)
                  .add("nRE", StringType, nullable = true)
                  .add("qExport", DoubleType, nullable = true)
                )
                .add("nDraw", StringType, nullable = true)
              ))
              .add("comb", new StructType()
                .add("CIDE", new StructType()
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vCIDE", DoubleType, nullable = true)
                )
                .add("CODIF", StringType, nullable = true)
                .add("UFCons", StringType, nullable = true)
                .add("cProdANP", StringType, nullable = true)
                .add("descANP", StringType, nullable = true)
                .add("encerrante", new StructType()
                  .add("nBico", StringType, nullable = true)
                  .add("nBomba", StringType, nullable = true)
                  .add("nTanque", StringType, nullable = true)
                  .add("vEncFin", DoubleType, nullable = true)
                  .add("vEncIni", DoubleType, nullable = true)
                )
                .add("origComb", ArrayType(new StructType()
                  .add("cUFOrig", StringType, nullable = true)
                  .add("indImport", StringType, nullable = true)
                  .add("pOrig", DoubleType, nullable = true)
                ))
                .add("pBio", DoubleType, nullable = true)
                .add("pGLP", DoubleType, nullable = true)
                .add("pGNi", DoubleType, nullable = true)
                .add("pGNn", DoubleType, nullable = true)
                .add("qTemp", DoubleType, nullable = true)
                .add("vPart", DoubleType, nullable = true)
              )
              .add("rastro", ArrayType(new StructType()
                .add("cAgreg", StringType, nullable = true)
                .add("dFab", StringType, nullable = true)
                .add("dVal", StringType, nullable = true)
                .add("nLote", StringType, nullable = true)
                .add("qLote", DoubleType, nullable = true)
              ))
            )
            .add("imposto", new StructType()
              .add("ICMS", new StructType()
                .add("ICMS00", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCP", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vFCP", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                )
                .add("ICMS02", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("adRemICMS", DoubleType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("qBCMono", DoubleType, nullable = true)
                  .add("vICMSMono", DoubleType, nullable = true)
                )
                .add("ICMS10", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("motDesICMSST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCP", DoubleType, nullable = true)
                  .add("pFCPST", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vBCFCP", DoubleType, nullable = true)
                  .add("vBCFCPST", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vFCP", DoubleType, nullable = true)
                  .add("vFCPST", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                  .add("vICMSSTDeson", DoubleType, nullable = true)
                )
                .add("ICMS15", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("adRemICMS", DoubleType, nullable = true)
                  .add("adRemICMSReten", DoubleType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("qBCMono", DoubleType, nullable = true)
                  .add("qBCMonoReten", DoubleType, nullable = true)
                  .add("vICMSMono", DoubleType, nullable = true)
                  .add("vICMSMonoReten", DoubleType, nullable = true)
                )
                .add("ICMS20", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("indDeduzDeson", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("motDesICMS", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCP", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pRedBC", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vBCFCP", DoubleType, nullable = true)
                  .add("vFCP", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSDeson", DoubleType, nullable = true)
                )
                .add("ICMS30", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("indDeduzDeson", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("motDesICMS", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCPST", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBCFCPST", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vFCPST", DoubleType, nullable = true)
                  .add("vICMSDeson", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                )
                .add("ICMS40", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("indDeduzDeson", StringType, nullable = true)
                  .add("motDesICMS", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("vICMSDeson", DoubleType, nullable = true)
                )
                .add("ICMS51", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pDif", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pRedBC", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSDif", DoubleType, nullable = true)
                  .add("vICMSOp", DoubleType, nullable = true)
                )
                .add("ICMS53", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("adRemICMS", DoubleType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pDif", DoubleType, nullable = true)
                  .add("qBCMono", DoubleType, nullable = true)
                  .add("vICMSMono", DoubleType, nullable = true)
                  .add("vICMSMonoDif", DoubleType, nullable = true)
                  .add("vICMSMonoOp", DoubleType, nullable = true)
                )
                .add("ICMS60", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCPSTRet", DoubleType, nullable = true)
                  .add("pICMSEfet", DoubleType, nullable = true)
                  .add("pRedBCEfet", DoubleType, nullable = true)
                  .add("pST", DoubleType, nullable = true)
                  .add("vBCEfet", DoubleType, nullable = true)
                  .add("vBCFCPSTRet", DoubleType, nullable = true)
                  .add("vBCSTRet", DoubleType, nullable = true)
                  .add("vFCPSTRet", DoubleType, nullable = true)
                  .add("vICMSEfet", DoubleType, nullable = true)
                  .add("vICMSSTRet", DoubleType, nullable = true)
                  .add("vICMSSubstituto", DoubleType, nullable = true)
                )
                .add("ICMS61", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("adRemICMSRet", DoubleType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("qBCMonoRet", DoubleType, nullable = true)
                  .add("vICMSMonoRet", DoubleType, nullable = true)
                )
                .add("ICMS70", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("indDeduzDeson", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("motDesICMS", StringType, nullable = true)
                  .add("motDesICMSST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBC", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSDeson", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                  .add("vICMSSTDeson", DoubleType, nullable = true)
                )
                .add("ICMS90", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("indDeduzDeson", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("motDesICMS", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCP", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBC", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vBCFCP", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vFCP", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSDeson", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                )
                .add("ICMSPart", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("UFST", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pBCOp", DoubleType, nullable = true)
                  .add("pFCPST", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pRedBC", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vBCFCPST", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vFCPST", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                )
                .add("ICMSSN101", new StructType()
                  .add("CSOSN", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pCredSN", DoubleType, nullable = true)
                  .add("vCredICMSSN", DoubleType, nullable = true)
                )
                .add("ICMSSN102", new StructType()
                  .add("CSOSN", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                )
                .add("ICMSSN201", new StructType()
                  .add("CSOSN", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pCredSN", DoubleType, nullable = true)
                  .add("pFCPST", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBCFCPST", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vCredICMSSN", DoubleType, nullable = true)
                  .add("vFCPST", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                )
                .add("ICMSSN202", new StructType()
                  .add("CSOSN", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCPST", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBCFCPST", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vFCPST", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                )
                .add("ICMSSN500", new StructType()
                  .add("CSOSN", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCPSTRet", DoubleType, nullable = true)
                  .add("pICMSEfet", DoubleType, nullable = true)
                  .add("pRedBCEfet", DoubleType, nullable = true)
                  .add("pST", DoubleType, nullable = true)
                  .add("vBCEfet", DoubleType, nullable = true)
                  .add("vBCFCPSTRet", DoubleType, nullable = true)
                  .add("vBCSTRet", DoubleType, nullable = true)
                  .add("vFCPSTRet", DoubleType, nullable = true)
                  .add("vICMSEfet", DoubleType, nullable = true)
                  .add("vICMSSTRet", DoubleType, nullable = true)
                  .add("vICMSSubstituto", DoubleType, nullable = true)
                )
                .add("ICMSSN900", new StructType()
                  .add("CSOSN", StringType, nullable = true)
                  .add("modBC", StringType, nullable = true)
                  .add("modBCST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pCredSN", DoubleType, nullable = true)
                  .add("pICMS", DoubleType, nullable = true)
                  .add("pICMSST", DoubleType, nullable = true)
                  .add("pMVAST", DoubleType, nullable = true)
                  .add("pRedBC", DoubleType, nullable = true)
                  .add("pRedBCST", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vBCST", DoubleType, nullable = true)
                  .add("vCredICMSSN", DoubleType, nullable = true)
                  .add("vICMS", DoubleType, nullable = true)
                  .add("vICMSST", DoubleType, nullable = true)
                )
                .add("ICMSST", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("orig", StringType, nullable = true)
                  .add("pFCPSTRet", DoubleType, nullable = true)
                  .add("pICMSEfet", DoubleType, nullable = true)
                  .add("pRedBCEfet", DoubleType, nullable = true)
                  .add("pST", DoubleType, nullable = true)
                  .add("vBCEfet", DoubleType, nullable = true)
                  .add("vBCFCPSTRet", DoubleType, nullable = true)
                  .add("vBCSTDest", DoubleType, nullable = true)
                  .add("vBCSTRet", DoubleType, nullable = true)
                  .add("vFCPSTRet", DoubleType, nullable = true)
                  .add("vICMSEfet", DoubleType, nullable = true)
                  .add("vICMSSTDest", DoubleType, nullable = true)
                  .add("vICMSSTRet", DoubleType, nullable = true)
                  .add("vICMSSubstituto", DoubleType, nullable = true)
                )
              )
              .add("ICMSUFDest", new StructType()
                .add("pFCPUFDest", DoubleType, nullable = true)
                .add("pICMSInter", DoubleType, nullable = true)
                .add("pICMSInterPart", DoubleType, nullable = true)
                .add("pICMSUFDest", DoubleType, nullable = true)
                .add("vBCFCPUFDest", DoubleType, nullable = true)
                .add("vBCUFDest", DoubleType, nullable = true)
                .add("vFCPUFDest", DoubleType, nullable = true)
                .add("vICMSUFDest", DoubleType, nullable = true)
                .add("vICMSUFRemet", DoubleType, nullable = true)
              )
              .add("II", new StructType()
                .add("vBC", DoubleType, nullable = true)
                .add("vDespAdu", DoubleType, nullable = true)
                .add("vII", DoubleType, nullable = true)
                .add("vIOF", DoubleType, nullable = true)
              )
              .add("IPI", new StructType()
                .add("CNPJProd", StringType, nullable = true)
                .add("IPINT", new StructType()
                  .add("CST", StringType, nullable = true)
                )
                .add("IPITrib", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("pIPI", DoubleType, nullable = true)
                  .add("qUnid", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vIPI", DoubleType, nullable = true)
                  .add("vUnid", DoubleType, nullable = true)
                )
                .add("cEnq", StringType, nullable = true)
                .add("cSelo", StringType, nullable = true)
                .add("qSelo", StringType, nullable = true)
              )
              .add("ISSQN", new StructType()
                .add("cListServ", DoubleType, nullable = true)
                .add("cMun", StringType, nullable = true)
                .add("cMunFG", StringType, nullable = true)
                .add("cPais", StringType, nullable = true)
                .add("cServico", StringType, nullable = true)
                .add("indISS", StringType, nullable = true)
                .add("indIncentivo", StringType, nullable = true)
                .add("vAliq", DoubleType, nullable = true)
                .add("vBC", DoubleType, nullable = true)
                .add("vISSQN", DoubleType, nullable = true)
              )
              .add("PIS", new StructType()
                .add("PISAliq", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("pPIS", DoubleType, nullable = true)
                  .add("vPIS", DoubleType, nullable = true)
                )
                .add("PISNT", new StructType()
                  .add("CST", StringType, nullable = true)
                )
                .add("PISOutr", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("pPIS", DoubleType, nullable = true)
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vPIS", DoubleType, nullable = true)
                )
                .add("PISQtde", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vPIS", DoubleType, nullable = true)
                )
                .add("PISST", new StructType()
                  .add("indSomaPISST", StringType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("pPIS", DoubleType, nullable = true)
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vPIS", DoubleType, nullable = true)
                )
                .add("vTotTrib", DoubleType, nullable = true)
              )
              .add("COFINS", new StructType()
                .add("COFINSAliq", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("pCOFINS", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vCOFINS", DoubleType, nullable = true)
                )
                .add("COFINSNT", new StructType()
                  .add("CST", StringType, nullable = true)
                )
                .add("COFINSOutr", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("pCOFINS", DoubleType, nullable = true)
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vCOFINS", DoubleType, nullable = true)
                )
                .add("COFINSQtde", new StructType()
                  .add("CST", StringType, nullable = true)
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vCOFINS", DoubleType, nullable = true)
                )
                .add("COFINSST", new StructType()
                  .add("indSomaCOFINSST", StringType, nullable = true)
                  .add("pCOFINS", DoubleType, nullable = true)
                  .add("qBCProd", DoubleType, nullable = true)
                  .add("vAliqProd", DoubleType, nullable = true)
                  .add("vBC", DoubleType, nullable = true)
                  .add("vCOFINS", DoubleType, nullable = true)
                )
              )
            )
            .add("impostoDevol", new StructType() // Nova estrutura
              .add("IPI", new StructType()
                .add("vIPIDevol", DoubleType, nullable = true)
              )
              .add("pDevol", DoubleType, nullable = true)
            )
            .add("infAdProd", StringType, nullable = true)
            .add("obsItem", new StructType()
              .add("obsCont", new StructType()
                .add("_xCampo", StringType, nullable = true)
                .add("xTexto", StringType, nullable = true)
              )
              .add("obsFisco", new StructType()
                .add("_xCampo", StringType, nullable = true)
                .add("xTexto", StringType, nullable = true)
              )
            )
          ))
        )
      )
  }
}

//NFCeDetProcessor.main(Array())
