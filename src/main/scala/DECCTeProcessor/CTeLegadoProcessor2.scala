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

object CTeLegadoProcessor2 {
    // Variáveis externas para o intervalo de meses e ano de processamento
    val anoInicio = 2025
    val anoFim = 2025
    val tipoDocumento = "cte"

    // Função para criar o esquema de forma modular

  import org.apache.spark.sql.types._

  import org.apache.spark.sql.types._

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
          $"parsed.infCte.emit.CNPJ".as("emit_cnpj"),
          $"parsed.infCte.emit.CPF".as("emit_cpf"),
          $"parsed.infCte.emit.CRT".as("emit_crt"),
          $"parsed.infCte.emit.IE".as("emit_ie"),
          $"parsed.infCte.emit.IEST".as("emit_iest"),
          $"parsed.infCte.emit.enderEmit.CEP".as("enderemit_cep"),
          $"parsed.infCte.emit.enderEmit.UF".as("enderemit_uf"),
          $"parsed.infCte.emit.enderEmit.cMun".as("enderemit_cmun"),
          $"parsed.infCte.emit.enderEmit.fone".as("enderemit_fone"),
          $"parsed.infCte.emit.enderEmit.nro".as("enderemit_nro"),
          $"parsed.infCte.emit.enderEmit.xBairro".as("enderemit_xbairro"),
          $"parsed.infCte.emit.enderEmit.xCpl".as("enderemit_xcpl"),
          $"parsed.infCte.emit.enderEmit.xLgr".as("enderemit_xlgr"),
          $"parsed.infCte.emit.enderEmit.xMun".as("enderemit_xmun"),
          $"parsed.infCte.emit.xFant".as("emit_xfant"),
          $"parsed.infCte.emit.xNome".as("emit_xnome"),
          $"parsed.infCte.dest.CNPJ".as("dest_cnpj"),
          $"parsed.infCte.dest.CPF".as("dest_cpf"),
          $"parsed.infCte.dest.IE".as("dest_ie"),
          $"parsed.infCte.dest.ISUF".as("dest_isuf"),
          $"parsed.infCte.dest.email".as("dest_email"),
          $"parsed.infCte.dest.enderDest.CEP".as("enderdest_cep"),
          $"parsed.infCte.dest.enderDest.UF".as("enderdest_uf"),
          $"parsed.infCte.dest.enderDest.cMun".as("enderdest_cmun"),
          $"parsed.infCte.dest.enderDest.cPais".as("enderdest_cpais"),
          $"parsed.infCte.dest.enderDest.nro".as("enderdest_nro"),
          $"parsed.infCte.dest.enderDest.xBairro".as("enderdest_xbairro"),
          $"parsed.infCte.dest.enderDest.xCpl".as("enderdest_xcpl"),
          $"parsed.infCte.dest.enderDest.xLgr".as("enderdest_xlgr"),
          $"parsed.infCte.dest.enderDest.xMun".as("enderdest_xmun"),
          $"parsed.infCte.dest.enderDest.xPais".as("enderdest_xpais"),
          $"parsed.infCte.dest.fone".as("dest_fone"),
          $"parsed.infCte.dest.xNome".as("dest_xnome"),
          $"parsed.infCte.ide.CFOP".as("ide_cfop"),
          $"parsed.infCte.ide.UFEnv".as("ide_ufenv"),
          $"parsed.infCte.ide.UFFim".as("ide_uffim"),
          $"parsed.infCte.ide.UFIni".as("ide_ufini"),
          $"parsed.infCte.ide.cCT".as("ide_cct"),
          $"parsed.infCte.ide.cDV".as("ide_cdv"),
          $"parsed.infCte.ide.cMunEnv".as("ide_cmunenv"),
          $"parsed.infCte.ide.cMunFim".as("ide_cmunfim"),
          $"parsed.infCte.ide.cMunIni".as("ide_cmunini"),
          $"parsed.infCte.ide.cUF".as("ide_cuf"),
          $"parsed.infCte.ide.dhCont".as("ide_dhcont"),
          $"parsed.infCte.ide.dhEmi".as("ide_dhemi"),
          $"parsed.infCte.ide.indGlobalizado".as("ide_indglobalizado"),
          $"parsed.infCte.ide.indIEToma".as("ide_indietoma"),
          $"parsed.infCte.ide.mod".as("ide_mod"),
          $"parsed.infCte.ide.modal".as("ide_modal"),
          $"parsed.infCte.ide.nCT".as("ide_nct"),
          $"parsed.infCte.ide.natOp".as("ide_natop"),
          $"parsed.infCte.ide.procEmi".as("ide_procemi"),
          $"parsed.infCte.ide.retira".as("ide_retira"),
          $"parsed.infCte.ide.serie".as("ide_serie"),
          $"parsed.infCte.ide.tpAmb".as("ide_tpamb"),
          $"parsed.infCte.ide.tpCTe".as("ide_tpcte"),
          $"parsed.infCte.ide.tpEmis".as("ide_tpemis"),
          $"parsed.infCte.ide.tpImp".as("ide_tpimp"),
          $"parsed.infCte.ide.tpServ".as("ide_tpserv"),
          $"parsed.infCte.ide.verProc".as("ide_verproc"),
          $"parsed.infCte.ide.xDetRetira".as("ide_xdetretira"),
          $"parsed.infCte.ide.xJust".as("ide_xjust"),
          $"parsed.infCte.ide.xMunEnv".as("ide_xmunenv"),
          $"parsed.infCte.ide.xMunFim".as("ide_xmunfim"),
          $"parsed.infCte.ide.xMunIni".as("ide_xmunini")
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
