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
package DECBPeProcessor

import Schemas.BPeSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object BpeProcLegadoAnualProcessor {
  // Variáveis externas para o intervalo de meses e ano de processamento
  val anoInicio = 2024
  val anoFim = 2024
  val tipoDocumento = "bpe"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractInfNFe").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe CTeOSSchema
    val schema = BPeSchema.createSchema()    // Lista de anos com base nas variáveis externas
    val anoList = (anoInicio to anoFim).map(_.toString).toList

    anoList.foreach { ano =>
      val parquetPath = s"/datalake/bronze/sources/dbms/dec/$tipoDocumento/201909_202501"

      // Registrar o horário de início da iteração
      val startTime = LocalDateTime.now()
      println(s"Início da iteração para $ano: $startTime")
      println(s"Lendo dados do caminho: $parquetPath")

      // 1. Carrega o arquivo Parquet
      val parquetDF = spark.read.parquet(parquetPath)

      // 2. Seleciona as colunas e filtra MODELO = 64
      val xmlDF = parquetDF
        .select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSU".cast("string").as("NSU"),
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR"
        )
      xmlDF.show()
      // 3. Usa `from_xml` para ler o XML da coluna usando o esquema
      val parsedDF = xmlDF.withColumn("parsed", from_xml($"xml", schema))
      //     parsedDF.printSchema()

      // 4. Seleciona os campos desejados
      val selectedDF = parsedDF.select(
        $"NSU",
        date_format(to_timestamp($"DHPROC", "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH").as("DHPROC_FORMATADO"),
        $"DHEMI",
        $"IP_TRANSMISSOR",
        $"parsed.BPe.infBPe._Id".as("infbpe_id"),
        $"parsed.BPe.infBPe._versao".as("infbpe_versao"),
        $"parsed.BPe.infBPe.agencia.CNPJ".as("agencia_cnpj"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.CEP".as("enderagencia_cep"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.UF".as("enderagencia_uf"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.cMun".as("enderagencia_cmun"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.cPais".as("enderagencia_cpais"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.email".as("enderagencia_email"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.fone".as("enderagencia_fone"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.nro".as("enderagencia_nro"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.xBairro".as("enderagencia_xbairro"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.xCpl".as("enderagencia_xcpl"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.xLgr".as("enderagencia_xlgr"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.xMun".as("enderagencia_xmun"),
        $"parsed.BPe.infBPe.agencia.enderAgencia.xPais".as("enderagencia_xpais"),
        $"parsed.BPe.infBPe.agencia.xNome".as("agencia_xnome"),
        $"parsed.BPe.infBPe.autXML.CNPJ".as("autxml_cnpj"),
        $"parsed.BPe.infBPe.comp.CPF".as("comp_cpf"),
        $"parsed.BPe.infBPe.comp.enderComp.CEP".as("endercomp_cep"),
        $"parsed.BPe.infBPe.comp.enderComp.UF".as("endercomp_uf"),
        $"parsed.BPe.infBPe.comp.enderComp.cMun".as("endercomp_cmun"),
        $"parsed.BPe.infBPe.comp.enderComp.cPais".as("endercomp_cpais"),
        $"parsed.BPe.infBPe.comp.enderComp.email".as("endercomp_email"),
        $"parsed.BPe.infBPe.comp.enderComp.fone".as("endercomp_fone"),
        $"parsed.BPe.infBPe.comp.enderComp.nro".as("endercomp_nro"),
        $"parsed.BPe.infBPe.comp.enderComp.xBairro".as("endercomp_xbairro"),
        $"parsed.BPe.infBPe.comp.enderComp.xCpl".as("endercomp_xcpl"),
        $"parsed.BPe.infBPe.comp.enderComp.xLgr".as("endercomp_xlgr"),
        $"parsed.BPe.infBPe.comp.enderComp.xMun".as("endercomp_xmun"),
        $"parsed.BPe.infBPe.comp.enderComp.xPais".as("endercomp_xpais"),
        $"parsed.BPe.infBPe.comp.xNome".as("comp_xnome"),
        $"parsed.BPe.infBPe.emit.CNAE".as("emit_cnae"),
        $"parsed.BPe.infBPe.emit.CNPJ".as("emit_cnpj"),
        $"parsed.BPe.infBPe.emit.CRT".as("emit_crt"),
        $"parsed.BPe.infBPe.emit.IE".as("emit_ie"),
        $"parsed.BPe.infBPe.emit.IEST".as("emit_iest"),
        $"parsed.BPe.infBPe.emit.IM".as("emit_im"),
        $"parsed.BPe.infBPe.emit.TAR".as("emit_tar"),
        $"parsed.BPe.infBPe.emit.enderEmit.CEP".as("enderemit_cep"),
        $"parsed.BPe.infBPe.emit.enderEmit.UF".as("enderemit_uf"),
        $"parsed.BPe.infBPe.emit.enderEmit.cMun".as("enderemit_cmun"),
        $"parsed.BPe.infBPe.emit.enderEmit.email".as("enderemit_email"),
        $"parsed.BPe.infBPe.emit.enderEmit.fone".as("enderemit_fone"),
        $"parsed.BPe.infBPe.emit.enderEmit.nro".as("enderemit_nro"),
        $"parsed.BPe.infBPe.emit.enderEmit.xBairro".as("enderemit_xbairro"),
        $"parsed.BPe.infBPe.emit.enderEmit.xCpl".as("enderemit_xcpl"),
        $"parsed.BPe.infBPe.emit.enderEmit.xLgr".as("enderemit_xlgr"),
        $"parsed.BPe.infBPe.emit.enderEmit.xMun".as("enderemit_xmun"),
        $"parsed.BPe.infBPe.emit.xFant".as("emit_xfant"),
        $"parsed.BPe.infBPe.emit.xNome".as("emit_xnome"),
        $"parsed.BPe.infBPe.ide.UFFim".as("ide_uffim"),
        $"parsed.BPe.infBPe.ide.UFIni".as("ide_ufini"),
        $"parsed.BPe.infBPe.ide.cBP".as("ide_cbp"),
        $"parsed.BPe.infBPe.ide.cDV".as("ide_cdv"),
        $"parsed.BPe.infBPe.ide.cMunFim".as("ide_cmunfim"),
        $"parsed.BPe.infBPe.ide.cMunIni".as("ide_cmunini"),
        $"parsed.BPe.infBPe.ide.cUF".as("ide_cuf"),
        $"parsed.BPe.infBPe.ide.dhCont".as("ide_dhcont"),
        $"parsed.BPe.infBPe.ide.dhEmi".as("ide_dhemi"),
        $"parsed.BPe.infBPe.ide.indPres".as("ide_indpres"),
        $"parsed.BPe.infBPe.ide.mod".as("ide_mod"),
        $"parsed.BPe.infBPe.ide.modal".as("ide_modal"),
        $"parsed.BPe.infBPe.ide.nBP".as("ide_nbp"),
        $"parsed.BPe.infBPe.ide.serie".as("ide_serie"),
        $"parsed.BPe.infBPe.ide.tpAmb".as("ide_tpamb"),
        $"parsed.BPe.infBPe.ide.tpBPe".as("ide_tpbpe"),
        $"parsed.BPe.infBPe.ide.tpEmis".as("ide_tpemis"),
        $"parsed.BPe.infBPe.ide.verProc".as("ide_verproc"),
        $"parsed.BPe.infBPe.ide.xJust".as("ide_xjust"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS00.CST".as("icms00_cst"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS00.pICMS".as("icms00_picms"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS00.vBC".as("icms00_vbc"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS00.vICMS".as("icms00_vicms"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS20.CST".as("icms20_cst"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS20.pICMS".as("icms20_picms"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS20.pRedBC".as("icms20_predbc"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS20.vBC".as("icms20_vbc"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS20.vICMS".as("icms20_vicms"),
        $"parsed.BPe.infBPe.imp.ICMS.ICMS45.CST".as("icms45_cst"),
        $"parsed.BPe.infBPe.imp.infAdFisco".as("imp_infadfisco"),
        $"parsed.BPe.infBPe.imp.vTotTrib".as("imp_vtottrib"),
        $"parsed.BPe.infBPe.infAdic.infAdFisco".as("infadic_infadfisco"),
        $"parsed.BPe.infBPe.infAdic.infCpl".as("infadic_infcpl"),
        $"parsed.BPe.infBPe.infBPeSub.chBPe".as("infbpesub_chbpe"),
        $"parsed.BPe.infBPe.infBPeSub.tpSub".as("infbpesub_tpsub"),
        $"parsed.BPe.infBPe.infPassagem.cLocDest".as("infpassagem_clocdest"),
        $"parsed.BPe.infBPe.infPassagem.cLocOrig".as("infpassagem_clocorig"),
        $"parsed.BPe.infBPe.infPassagem.dhEmb".as("infpassagem_dhemb"),
        $"parsed.BPe.infBPe.infPassagem.dhValidade".as("infpassagem_dhvalidade"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.CPF".as("infpassageiro_cpf"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.dNasc".as("infpassageiro_dnasc"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.email".as("infpassageiro_email"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.fone".as("infpassageiro_fone"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.nDoc".as("infpassageiro_ndoc"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.tpDoc".as("infpassageiro_tpdoc"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.xDoc".as("infpassageiro_xdoc"),
        $"parsed.BPe.infBPe.infPassagem.infPassageiro.xNome".as("infpassageiro_xnome"),
        $"parsed.BPe.infBPe.infPassagem.xLocDest".as("infpassagem_xlocdest"),
        $"parsed.BPe.infBPe.infPassagem.xLocOrig".as("infpassagem_xlocorig"),
        $"parsed.BPe.infBPe.infRespTec.CNPJ".as("infresptec_cnpj"),
        $"parsed.BPe.infBPe.infRespTec.email".as("infresptec_email"),
        $"parsed.BPe.infBPe.infRespTec.fone".as("infresptec_fone"),
        $"parsed.BPe.infBPe.infRespTec.xContato".as("infresptec_xcontato"),
        $"parsed.BPe.infBPe.infValorBPe.Comp".as("infvalorbpe_comp"), // Array intacto
        $"parsed.BPe.infBPe.infValorBPe.cDesconto".as("infvalorbpe_cdesconto"),
        $"parsed.BPe.infBPe.infValorBPe.tpDesconto".as("infvalorbpe_tpdesconto"),
        $"parsed.BPe.infBPe.infValorBPe.vBP".as("infvalorbpe_vbp"),
        $"parsed.BPe.infBPe.infValorBPe.vDesconto".as("infvalorbpe_vdesconto"),
        $"parsed.BPe.infBPe.infValorBPe.vPgto".as("infvalorbpe_vpgto"),
        $"parsed.BPe.infBPe.infValorBPe.vTroco".as("infvalorbpe_vtroco"),
        $"parsed.BPe.infBPe.infValorBPe.xDesconto".as("infvalorbpe_xdesconto"),
        $"parsed.BPe.infBPe.infViagem.cPercurso".as("infviagem_cpercurso"),
        $"parsed.BPe.infBPe.infViagem.dhViagem".as("infviagem_dhviagem"),
        $"parsed.BPe.infBPe.infViagem.infTravessia.sitVeiculo".as("inftravessia_sitveiculo"),
        $"parsed.BPe.infBPe.infViagem.infTravessia.tpVeiculo".as("inftravessia_tpveiculo"),
        $"parsed.BPe.infBPe.infViagem.plataforma".as("infviagem_plataforma"),
        $"parsed.BPe.infBPe.infViagem.poltrona".as("infviagem_poltrona"),
        $"parsed.BPe.infBPe.infViagem.prefixo".as("infviagem_prefixo"),
        $"parsed.BPe.infBPe.infViagem.tpAcomodacao".as("infviagem_tpacomodacao"),
        $"parsed.BPe.infBPe.infViagem.tpServ".as("infviagem_tpserv"),
        $"parsed.BPe.infBPe.infViagem.tpTrecho".as("infviagem_tptrecho"),
        $"parsed.BPe.infBPe.infViagem.tpViagem".as("infviagem_tpviagem"),
        $"parsed.BPe.infBPe.infViagem.xPercurso".as("infviagem_xpercurso"),
        $"parsed.BPe.infBPe.pag".as("infbpe_pag"), // Array intacto
        $"parsed.BPe.infBPeSupl.boardPassBPe".as("infbpesupl_boardpassbpe"),
        $"parsed.BPe.infBPeSupl.qrCodBPe".as("infbpesupl_qrcodbpe"),
        $"parsed._dhConexao".as("dhconexao"),
        $"parsed._ipTransmissor".as("iptransmissor"),
        $"parsed._nPortaCon".as("nportacon"),
        $"parsed._versao".as("versao"),
        $"parsed._xmlns".as("xmlns"),
        $"parsed.protBPe._versao".as("protbpe_versao"),
        $"parsed.protBPe.infProt._Id".as("infprot_id"),
        $"parsed.protBPe.infProt.cStat".as("infprot_cstat"),
        $"parsed.protBPe.infProt.chBPe".as("chave"),
        $"parsed.protBPe.infProt.dhRecbto".as("infprot_dhrecbto"),
        $"parsed.protBPe.infProt.digVal".as("infprot_digval"),
        $"parsed.protBPe.infProt.nProt".as("infprot_nprot"),
        $"parsed.protBPe.infProt.tpAmb".as("infprot_tpamb"),
        $"parsed.protBPe.infProt.verAplic".as("infprot_veraplic"),
        $"parsed.protBPe.infProt.xMotivo".as("infprot_xmotivo")
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
      val repartitionedDF = selectedDFComParticao.repartition(1)

      // Escrever os dados particionados
      repartitionedDF
        .write.mode("append")
        .format("parquet")
        .option("compression", "lz4")
        .option("parquet.block.size", 500 * 1024 * 1024) // 500 MB
        .partitionBy("chave_particao") // Garante a separação por partição
        .save("/datalake/prata/sources/dbms/dec/bpe/BPe")

      // Registrar o horário de término da gravação
      val saveEndTime = LocalDateTime.now()
      println(s"Gravação concluída: $saveEndTime")
    }
  }
}

//BpeProcLegadoAnualProcessor.main(Array())
