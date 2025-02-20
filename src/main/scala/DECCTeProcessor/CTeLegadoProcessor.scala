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
import org.apache.spark.sql.types._
import java.time.LocalDateTime

object CTeLegadoProcessor {
    // Variáveis externas para o intervalo de meses e ano de processamento
    val anoInicio = 2025
    val anoFim = 2025
    val tipoDocumento = "cte"

    // Função para criar o esquema de forma modular

  def createSchema(): StructType = {
    // Insira o esquema aqui
    new StructType()
      .add("protCTe", new StructType()
        .add("infProt", new StructType()
          .add("_Id", StringType, nullable = true)
          .add("chCTe", StringType, nullable = true)
          .add("cStat", StringType, nullable = true)
          .add("dhRecbto", StringType, nullable = true)
          .add("digVal", StringType, nullable = true)
          .add("nProt", StringType, nullable = true)
          .add("tpAmb", StringType, nullable = true)
          .add("verAplic", StringType, nullable = true)
          .add("xMotivo", StringType, nullable = true)
        )
      )
      .add("CTe", new StructType()
        .add("infCte", new StructType()
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
            .add("dhCont", StringType, true)
            .add("dhEmi", StringType, true)
            .add("indGlobalizado", StringType, true)
            .add("indIEToma", StringType, true)
            .add("mod", StringType, true)
            .add("modal", StringType, true)
            .add("nCT", StringType, true)
            .add("natOp", StringType, true)
            .add("procEmi", StringType, true)
            .add("retira", StringType, true)
            .add("serie", StringType, true)
            .add("toma3", new StructType()
              .add("toma", StringType, true)
              , true)
            .add("toma4", new StructType()
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
                .add("xPais", StringType, true)
                , true)
              .add("fone", StringType, true)
              .add("toma", StringType, true)
              .add("xFant", StringType, true)
              .add("xNome", StringType, true)
              , true)
            .add("tpAmb", StringType, true)
            .add("tpCTe", StringType, true)
            .add("tpEmis", StringType, true)
            .add("tpImp", StringType, true)
            .add("tpServ", StringType, true)
            .add("verProc", StringType, true)
            .add("xDetRetira", StringType, true)
            .add("xJust", StringType, nullable = true)
            .add("xMunEnv", StringType, true)
            .add("xMunFim", StringType, true)
            .add("xMunIni", StringType, true)
            , true)
          .add("emit", new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true)
            .add("CRT", StringType, true)
            .add("IE", StringType, true)
            .add("IEST", StringType, true)
            .add("enderEmit", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("fone", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true)
              , true)
            .add("xFant", StringType, true)
            .add("xNome", StringType, true)
            , true)
          .add("dest", new StructType()
            .add("CNPJ", StringType, true)
            .add("CPF", StringType, true)
            .add("IE", StringType, true)
            .add("ISUF", StringType, true)
            .add("email", StringType, true)
            .add("enderDest", new StructType()
              .add("CEP", StringType, true)
              .add("UF", StringType, true)
              .add("cMun", StringType, true)
              .add("cPais", StringType, true)
              .add("nro", StringType, true)
              .add("xBairro", StringType, true)
              .add("xCpl", StringType, true)
              .add("xLgr", StringType, true)
              .add("xMun", StringType, true)
              .add("xPais", StringType, true)
              , true)
            .add("fone", StringType, true)
            .add("xNome", StringType, true)
            , true)
          .add("rem", StringType, true)
          .add("vPrest", StringType, true)
          , true)
      )
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

        // 2. Seleciona as colunas XML_DOCUMENTO_CLOB e NSUSVD
        val xmlDF = parquetDF.select(
          $"XML_DOCUMENTO_CLOB".cast("string").as("xml"),
          $"NSUSVD",
          $"DHPROC",
          $"DHEMI",
          $"IP_TRANSMISSOR",
          $"MODELO",
          $"TPEMIS"
        )

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
          $"parsed.CTe.infCte.ide.xMunIni".as("ide_xmunini")
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
          .save("/datalake/prata/sources/dbms/dec/cte/infCTe")

        // Registrar o horário de término da gravação
        val saveEndTime = LocalDateTime.now()
        println(s"Gravação concluída: $saveEndTime")    }
    }
  }

//CTeLegadoProcessor.main(Array())
