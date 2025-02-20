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
import org.apache.spark.sql.types.{StringType, StructType}

import java.time.LocalDateTime

object GVTeProcLegadoProcessor {
    // Variáveis externas para o intervalo de meses e ano de processamento
    val anoInicio = 2025
    val anoFim = 2025
    val tipoDocumento = "cte"

    // Função para criar o esquema de forma modular

  import org.apache.spark.sql.types._

  def createSchema(): StructType = {
    new StructType()
    .add("GTVe", new StructType()
      .add("_versao", DoubleType, true)
      .add("infCTeSupl", new StructType()
        .add("qrCodCTe", StringType, true))
      .add("infCte", new StructType()
        .add("_Id", StringType, true)
        .add("_versao", DoubleType, true)
        .add("compl", new StructType()
          .add("xEmi", StringType, true)
          .add("xObs", StringType, true))
        .add("dest", new StructType()
          .add("CNPJ", LongType, true)
          .add("IE", StringType, true)
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
          .add("xNome", StringType, true))
        .add("destino", new StructType()
          .add("CEP", LongType, true)
          .add("UF", StringType, true)
          .add("cMun", LongType, true)
          .add("nro", StringType, true)
          .add("xBairro", StringType, true)
          .add("xCpl", StringType, true)
          .add("xLgr", StringType, true)
          .add("xMun", StringType, true))
        .add("detGTV", new StructType()
          .add("infEspecie", ArrayType(new StructType()
            .add("tpEspecie", LongType, true)
            .add("tpNumerario", LongType, true)
            .add("vEspecie", DoubleType, true)
            .add("xMoedaEstr", StringType, true)), true)
          .add("infVeiculo", new StructType()
            .add("RNTRC", LongType, true)
            .add("UF", StringType, true)
            .add("placa", StringType, true))
          .add("qCarga", DoubleType, true))
        .add("emit", new StructType()
          .add("CNPJ", LongType, true)
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
          .add("cCT", LongType, true)
          .add("cDV", LongType, true)
          .add("cMunEnv", LongType, true)
          .add("cUF", LongType, true)
          .add("dhChegadaDest", TimestampType, true)
          .add("dhEmi", TimestampType, true)
          .add("dhSaidaOrig", TimestampType, true)
          .add("indIEToma", LongType, true)
          .add("mod", LongType, true)
          .add("modal", LongType, true)
          .add("nCT", LongType, true)
          .add("natOp", StringType, true)
          .add("serie", LongType, true)
          .add("toma", new StructType()
            .add("toma", LongType, true))
          .add("tomaTerceiro", new StructType()
            .add("CNPJ", LongType, true)
            .add("IE", StringType, true)
            .add("enderToma", new StructType()
              .add("CEP", LongType, true)
              .add("UF", StringType, true)
              .add("cMun", LongType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true))
            .add("toma", LongType, true)
            .add("xNome", StringType, true))
          .add("tpAmb", LongType, true)
          .add("tpCTe", LongType, true)
          .add("tpEmis", LongType, true)
          .add("tpImp", LongType, true)
          .add("tpServ", LongType, true)
          .add("verProc", StringType, true)
          .add("xMunEnv", StringType, true))
        .add("infRespTec", new StructType()
          .add("CNPJ", LongType, true)
          .add("email", StringType, true)
          .add("fone", LongType, true)
          .add("xContato", StringType, true))
        .add("origem", new StructType()
          .add("CEP", LongType, true)
          .add("UF", StringType, true)
          .add("cMun", LongType, true)
          .add("nro", StringType, true)
          .add("xBairro", StringType, true)
          .add("xCpl", StringType, true)
          .add("xLgr", StringType, true)
          .add("xMun", StringType, true))
        .add("rem", new StructType()
          .add("CNPJ", LongType, true)
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
          .add("xNome", StringType, true))))
      .add("_dhConexao", TimestampType, true)
      .add("_ipTransmissor", StringType, true)
      .add("_nPortaCon", LongType, true)
      .add("_versao", DoubleType, true)
      .add("_xmlns", StringType, true)
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

        // 2. Seleciona as colunas e filtra MODELO = 64
        val xmlDF = parquetDF
          .filter($"MODELO" === 64) // Filtra onde MODELO é igual a 64
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
          $"parsed.protCTe._versao".as("protcte_versao"),
          $"parsed.protCTe.infProt._Id".as("infprot_id"),
          $"parsed.protCTe.infProt.cStat".as("infprot_cstat"),
          $"parsed.protCTe.infProt.chCTe".as("chave"),
          $"parsed.protCTe.infProt.dhRecbto".as("infprot_dhrecbto"),
          $"parsed.protCTe.infProt.digVal".as("infprot_digval"),
          $"parsed.protCTe.infProt.nProt".as("infprot_nprot"),
          $"parsed.protCTe.infProt.tpAmb".as("infprot_tpamb"),
          $"parsed.protCTe.infProt.verAplic".as("infprot_veraplic"),
          $"parsed.GTVe._versao".as("gtve_versao"),
          $"parsed.GTVe.infCTeSupl.qrCodCTe".as("gtve_infctesupl_qrcodcte"),
          $"parsed.GTVe.infCte._Id".as("gtve_infcte_id"),
          $"parsed.GTVe.infCte._versao".as("gtve_infcte_versao"),
          $"parsed.GTVe.infCte.compl.xEmi".as("gtve_infcte_compl_xemi"),
          $"parsed.GTVe.infCte.compl.xObs".as("gtve_infcte_compl_xobs"),
          $"parsed.GTVe.infCte.dest.CNPJ".as("gtve_infcte_dest_cnpj"),
          $"parsed.GTVe.infCte.dest.IE".as("gtve_infcte_dest_ie"),
          $"parsed.GTVe.infCte.dest.xNome".as("gtve_infcte_dest_xnome"),
          $"parsed.GTVe.infCte.dest.enderDest.CEP".as("gtve_infcte_dest_enderdest_cep"),
          $"parsed.GTVe.infCte.dest.enderDest.UF".as("gtve_infcte_dest_enderdest_uf"),
          $"parsed.GTVe.infCte.dest.enderDest.cMun".as("gtve_infcte_dest_enderdest_cmun"),
          $"parsed.GTVe.infCte.dest.enderDest.cPais".as("gtve_infcte_dest_enderdest_cpais"),
          $"parsed.GTVe.infCte.dest.enderDest.nro".as("gtve_infcte_dest_enderdest_nro"),
          $"parsed.GTVe.infCte.dest.enderDest.xBairro".as("gtve_infcte_dest_enderdest_xbairro"),
          $"parsed.GTVe.infCte.dest.enderDest.xCpl".as("gtve_infcte_dest_enderdest_xcpl"),
          $"parsed.GTVe.infCte.dest.enderDest.xLgr".as("gtve_infcte_dest_enderdest_xlgr"),
          $"parsed.GTVe.infCte.dest.enderDest.xMun".as("gtve_infcte_dest_enderdest_xmun"),
          $"parsed.GTVe.infCte.dest.enderDest.xPais".as("gtve_infcte_dest_enderdest_xpais"),
          $"parsed.GTVe.infCte.emit.CNPJ".as("gtve_infcte_emit_cnpj"),
          $"parsed.GTVe.infCte.emit.IE".as("gtve_infcte_emit_ie"),
          $"parsed.GTVe.infCte.emit.xFant".as("gtve_infcte_emit_xfant"),
          $"parsed.GTVe.infCte.emit.xNome".as("gtve_infcte_emit_xnome"),
          $"parsed.GTVe.infCte.emit.enderEmit.CEP".as("gtve_infcte_emit_enderemit_cep"),
          $"parsed.GTVe.infCte.emit.enderEmit.UF".as("gtve_infcte_emit_enderemit_uf"),
          $"parsed.GTVe.infCte.emit.enderEmit.cMun".as("gtve_infcte_emit_enderemit_cmun"),
          $"parsed.GTVe.infCte.emit.enderEmit.fone".as("gtve_infcte_emit_enderemit_fone"),
          $"parsed.GTVe.infCte.emit.enderEmit.nro".as("gtve_infcte_emit_enderemit_nro"),
          $"parsed.GTVe.infCte.emit.enderEmit.xBairro".as("gtve_infcte_emit_enderemit_xbairro"),
          $"parsed.GTVe.infCte.emit.enderEmit.xCpl".as("gtve_infcte_emit_enderemit_xcpl"),
          $"parsed.GTVe.infCte.emit.enderEmit.xLgr".as("gtve_infcte_emit_enderemit_xlgr"),
          $"parsed.GTVe.infCte.emit.enderEmit.xMun".as("gtve_infcte_emit_enderemit_xmun"),
          $"parsed.GTVe.infCte.ide.CFOP".as("gtve_infcte_ide_cfop"),
          $"parsed.GTVe.infCte.ide.UFEnv".as("gtve_infcte_ide_ufenv"),
          $"parsed.GTVe.infCte.ide.cMunEnv".as("gtve_infcte_ide_cmunenv"),
          $"parsed.GTVe.infCte.ide.dhEmi".as("gtve_infcte_ide_dhemi"),
          $"parsed.GTVe.infCte.ide.nCT".as("gtve_infcte_ide_nct"),
          $"parsed.GTVe.infCte.ide.natOp".as("gtve_infcte_ide_natop"),
          $"parsed.GTVe.infCte.ide.serie".as("gtve_infcte_ide_serie"),
          $"parsed.GTVe.infCte.ide.tpAmb".as("gtve_infcte_ide_tpamb"),
          $"parsed.GTVe.infCte.ide.tpCTe".as("gtve_infcte_ide_tpcte"),
          $"parsed.GTVe.infCte.ide.tpEmis".as("gtve_infcte_ide_tpemis"),
          $"parsed.GTVe.infCte.ide.verProc".as("gtve_infcte_ide_verproc"),
          $"parsed._dhConexao".as("dhconexao"),
          $"parsed._ipTransmissor".as("iptransmissor"),
          $"parsed._nPortaCon".as("nportacon"),
          $"parsed._versao".as("versao"),
          $"parsed._xmlns".as("xmlns")
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
          .save("/datalake/prata/sources/dbms/dec/cte/GVTe")

        // Registrar o horário de término da gravação
        val saveEndTime = LocalDateTime.now()
        println(s"Gravação concluída: $saveEndTime")    }
    }
  }

//GVTeProcLegadoProcessor.main(Array())
