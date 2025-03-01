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
package DECGVTeProcessor

import Schemas.GVTeSchema
import com.databricks.spark.xml.functions.from_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object GVTeProcLegadoAnualProcessor {
    // Variáveis externas para o intervalo de meses e ano de processamento
    val anoInicio = 2020
    val anoFim = 2024
    val tipoDocumento = "cte"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExtractGTVe").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // Obter o esquema da classe CTeOSSchema
    val schema = GVTeSchema.createSchema()
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
            $"NSUSVD".cast("string").as("NSUSVD"),
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
          $"parsed.protCTe.infProt._Id".as("infProt_id"),
          $"parsed.protCTe.infProt.cStat".as("infprot_cstat"),
          $"parsed.protCTe.infProt.chCTe".as("chave"),
          $"parsed.protCTe.infProt.dhRecbto".as("infprot_dhrecbto"),
          $"parsed.protCTe.infProt.digVal".as("infprot_digval"),
          $"parsed.protCTe.infProt.nProt".as("infprot_nprot"),
          $"parsed.protCTe.infProt.tpAmb".as("infprot_tpamb"),
          $"parsed.protCTe.infProt.verAplic".as("infprot_veraplic"),
          $"parsed.protCTe.infProt.xMotivo".as("infprot_xmotivo"),
          $"parsed.GTVe.infCTeSupl.qrCodCTe".as("infctesupl_qrcodcte"),
          $"parsed.GTVe.infCte._Id".as("infCte_id"),
          $"parsed.GTVe.infCte._versao".as("infCte_versao"),
          $"parsed.GTVe.infCte.compl.xEmi".as("compl_xemi"),
          $"parsed.GTVe.infCte.compl.xObs".as("compl_xobs"),
          $"parsed.GTVe.infCte.dest.CNPJ".as("dest_cnpj"),
          $"parsed.GTVe.infCte.dest.IE".as("dest_ie"),
          $"parsed.GTVe.infCte.dest.enderDest.CEP".as("enderdest_cep"),
          $"parsed.GTVe.infCte.dest.enderDest.UF".as("enderdest_uf"),
          $"parsed.GTVe.infCte.dest.enderDest.cMun".as("enderdest_cmun"),
          $"parsed.GTVe.infCte.dest.enderDest.cPais".as("enderdest_cpais"),
          $"parsed.GTVe.infCte.dest.enderDest.nro".as("enderdest_nro"),
          $"parsed.GTVe.infCte.dest.enderDest.xBairro".as("enderdest_xbairro"),
          $"parsed.GTVe.infCte.dest.enderDest.xCpl".as("enderdest_xcpl"),
          $"parsed.GTVe.infCte.dest.enderDest.xLgr".as("enderdest_xlgr"),
          $"parsed.GTVe.infCte.dest.enderDest.xMun".as("enderdest_xmun"),
          $"parsed.GTVe.infCte.dest.enderDest.xPais".as("enderdest_xpais"),
          $"parsed.GTVe.infCte.dest.xNome".as("dest_xnome"),
          $"parsed.GTVe.infCte.destino.CEP".as("destino_cep"),
          $"parsed.GTVe.infCte.destino.UF".as("destino_uf"),
          $"parsed.GTVe.infCte.destino.cMun".as("destino_cmun"),
          $"parsed.GTVe.infCte.destino.nro".as("destino_nro"),
          $"parsed.GTVe.infCte.destino.xBairro".as("destino_xbairro"),
          $"parsed.GTVe.infCte.destino.xCpl".as("destino_xcpl"),
          $"parsed.GTVe.infCte.destino.xLgr".as("destino_xlgr"),
          $"parsed.GTVe.infCte.destino.xMun".as("destino_xmun"),
          $"parsed.GTVe.infCte.detGTV.infEspecie".as("detgtv_infespecie"),
          $"parsed.GTVe.infCte.detGTV.infVeiculo.RNTRC".as("infveiculo_rntrc"),
          $"parsed.GTVe.infCte.detGTV.infVeiculo.UF".as("infveiculo_uf"),
          $"parsed.GTVe.infCte.detGTV.infVeiculo.placa".as("infveiculo_placa"),
          $"parsed.GTVe.infCte.detGTV.qCarga".as("detgtv_qcarga"),
          $"parsed.GTVe.infCte.emit.CNPJ".as("emit_cnpj"),
          $"parsed.GTVe.infCte.emit.IE".as("emit_ie"),
          $"parsed.GTVe.infCte.emit.enderEmit.CEP".as("enderemit_cep"),
          $"parsed.GTVe.infCte.emit.enderEmit.UF".as("enderemit_uf"),
          $"parsed.GTVe.infCte.emit.enderEmit.cMun".as("enderemit_cmun"),
          $"parsed.GTVe.infCte.emit.enderEmit.fone".as("enderemit_fone"),
          $"parsed.GTVe.infCte.emit.enderEmit.nro".as("enderemit_nro"),
          $"parsed.GTVe.infCte.emit.enderEmit.xBairro".as("enderemit_xbairro"),
          $"parsed.GTVe.infCte.emit.enderEmit.xCpl".as("enderemit_xcpl"),
          $"parsed.GTVe.infCte.emit.enderEmit.xLgr".as("enderemit_xlgr"),
          $"parsed.GTVe.infCte.emit.enderEmit.xMun".as("enderemit_xmun"),
          $"parsed.GTVe.infCte.emit.xFant".as("emit_xfant"),
          $"parsed.GTVe.infCte.emit.xNome".as("emit_xnome"),
          $"parsed.GTVe.infCte.ide.CFOP".as("ide_cfop"),
          $"parsed.GTVe.infCte.ide.UFEnv".as("ide_ufenv"),
          $"parsed.GTVe.infCte.ide.cCT".as("ide_cct"),
          $"parsed.GTVe.infCte.ide.cDV".as("ide_cdv"),
          $"parsed.GTVe.infCte.ide.cMunEnv".as("ide_cmunenv"),
          $"parsed.GTVe.infCte.ide.cUF".as("ide_cuf"),
          $"parsed.GTVe.infCte.ide.dhChegadaDest".as("ide_dhchegadadest"),
          $"parsed.GTVe.infCte.ide.dhEmi".as("ide_dhemi"),
          $"parsed.GTVe.infCte.ide.dhSaidaOrig".as("ide_dhsaidaorig"),
          $"parsed.GTVe.infCte.ide.indIEToma".as("ide_indietoma"),
          $"parsed.GTVe.infCte.ide.mod".as("ide_mod"),
          $"parsed.GTVe.infCte.ide.modal".as("ide_modal"),
          $"parsed.GTVe.infCte.ide.nCT".as("ide_nct"),
          $"parsed.GTVe.infCte.ide.natOp".as("ide_natop"),
          $"parsed.GTVe.infCte.ide.serie".as("ide_serie"),
          $"parsed.GTVe.infCte.ide.toma.toma".as("toma_toma"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.CNPJ".as("tomaterceiro_cnpj"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.IE".as("tomaterceiro_ie"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.CEP".as("endertoma_cep"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.UF".as("endertoma_uf"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.cMun".as("endertoma_cmun"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.nro".as("endertoma_nro"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xBairro".as("endertoma_xbairro"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xCpl".as("endertoma_xcpl"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xLgr".as("endertoma_xlgr"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.enderToma.xMun".as("endertoma_xmun"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.toma".as("tomaterceiro_toma"),
          $"parsed.GTVe.infCte.ide.tomaTerceiro.xNome".as("tomaterceiro_xnome"),
          $"parsed.GTVe.infCte.ide.tpAmb".as("ide_tpamb"),
          $"parsed.GTVe.infCte.ide.tpCTe".as("ide_tpcte"),
          $"parsed.GTVe.infCte.ide.tpEmis".as("ide_tpemis"),
          $"parsed.GTVe.infCte.ide.tpImp".as("ide_tpimp"),
          $"parsed.GTVe.infCte.ide.tpServ".as("ide_tpserv"),
          $"parsed.GTVe.infCte.ide.verProc".as("ide_verproc"),
          $"parsed.GTVe.infCte.ide.xMunEnv".as("ide_xmunenv"),
          $"parsed.GTVe.infCte.infRespTec.CNPJ".as("infresptec_cnpj"),
          $"parsed.GTVe.infCte.infRespTec.email".as("infresptec_email"),
          $"parsed.GTVe.infCte.infRespTec.fone".as("infresptec_fone"),
          $"parsed.GTVe.infCte.infRespTec.xContato".as("infresptec_xcontato"),
          $"parsed.GTVe.infCte.origem.CEP".as("origem_cep"),
          $"parsed.GTVe.infCte.origem.UF".as("origem_uf"),
          $"parsed.GTVe.infCte.origem.cMun".as("origem_cmun"),
          $"parsed.GTVe.infCte.origem.nro".as("origem_nro"),
          $"parsed.GTVe.infCte.origem.xBairro".as("origem_xbairro"),
          $"parsed.GTVe.infCte.origem.xCpl".as("origem_xcpl"),
          $"parsed.GTVe.infCte.origem.xLgr".as("origem_xlgr"),
          $"parsed.GTVe.infCte.origem.xMun".as("origem_xmun"),
          $"parsed.GTVe.infCte.rem.CNPJ".as("rem_cnpj"),
          $"parsed.GTVe.infCte.rem.IE".as("rem_ie"),
          $"parsed.GTVe.infCte.rem.email".as("rem_email"),
          $"parsed.GTVe.infCte.rem.enderReme.CEP".as("enderreme_cep"),
          $"parsed.GTVe.infCte.rem.enderReme.UF".as("enderreme_uf"),
          $"parsed.GTVe.infCte.rem.enderReme.cMun".as("enderreme_cmun"),
          $"parsed.GTVe.infCte.rem.enderReme.cPais".as("enderreme_cpais"),
          $"parsed.GTVe.infCte.rem.enderReme.nro".as("enderreme_nro"),
          $"parsed.GTVe.infCte.rem.enderReme.xBairro".as("enderreme_xbairro"),
          $"parsed.GTVe.infCte.rem.enderReme.xCpl".as("enderreme_xcpl"),
          $"parsed.GTVe.infCte.rem.enderReme.xLgr".as("enderreme_xlgr"),
          $"parsed.GTVe.infCte.rem.enderReme.xMun".as("enderreme_xmun"),
          $"parsed.GTVe.infCte.rem.enderReme.xPais".as("enderreme_xpais"),
          $"parsed.GTVe.infCte.rem.fone".as("rem_fone"),
          $"parsed.GTVe.infCte.rem.xFant".as("rem_xfant"),
          $"parsed.GTVe.infCte.rem.xNome".as("rem_xnome")
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
          .save("/datalake/prata/sources/dbms/dec/cte/GVTe")

        // Registrar o horário de término da gravação
        val saveEndTime = LocalDateTime.now()
        println(s"Gravação concluída: $saveEndTime")    }
    }
  }

//GVTeProcLegadoAnualProcessor.main(Array())
