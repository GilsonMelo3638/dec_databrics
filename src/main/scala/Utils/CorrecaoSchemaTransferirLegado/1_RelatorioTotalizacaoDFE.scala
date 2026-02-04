package Utils.CorrecaoSchemaTransferirLegado

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import java.util.Properties

object RelatorioTotalizacaoDFE {
  // Datas para o relatório - VARIÁVEIS CENTRALIZADAS E ÚNICAS
  val DATA_INICIO = "01/01/2026"
  val DATA_FIM = "31/01/2026"

  def main(args: Array[String]): Unit = {
    // Inicializa a sessão do Spark
    val spark = SparkSession.builder()
      .appName("RelatorioTotalizacaoDFE")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    import spark.implicits._

    // Configurações de conexão com o banco de dados Oracle
    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admhadoop")
    connectionProperties.put("password", ".admhadoop#")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    // Formatar as datas para o padrão do Oracle
    val dataInicial = s"$DATA_INICIO 00:00:00"
    val dataFinal = s"$DATA_FIM 23:59:59"

    // Lista de tabelas e suas descrições para o relatório - CORRIGIDA
    val tabelas = Seq(
      ("ADMDEC.DEC_DFE_NFE", "NF-e (Notas Fiscais Eletrônicas)"),
      ("ADMDEC.DEC_DFE_NFE_CANCELAMENTO", "Cancelamentos de NF-e"),
      ("ADMDEC.DEC_DFE_NFE_EVENTO", "Eventos de NF-e (exceto cancelamentos)"),
      ("ADMDEC.DEC_DFE_NFCE", "NFC-e (Notas Fiscais de Consumidor Eletrônicas)"),
      ("ADMDEC.DEC_DFE_NFCE_CANCELAMENTO", "Cancelamentos de NFC-e"),
      ("ADMDEC.DEC_DFE_NF3E", "NF3-e (Notas Fiscais de Energia Elétrica)"),
      ("ADMDEC.DEC_DFE_NF3E_CANCELAMENTO", "Cancelamentos de NF3-e"),
      ("ADMDEC.DEC_DFE_NF3E_EVENTO", "Eventos de NF3-e"),
      ("ADMDEC.DEC_DFE_NFCOM", "NFCom (Notas Fiscais de Comunicação)"),
      ("ADMDEC.DEC_DFE_NFCOM_CANCELAMENTO", "Cancelamentos de NFCom"),
      ("ADMDEC.DEC_DFE_NFCOM_EVENTO", "Eventos de NFCom"),
      ("ADMDEC.DEC_DFE_BPE", "BPE (Bilhetes de Passagem Eletrônicos)"),
      ("ADMDEC.DEC_DFE_BPE_CANCELAMENTO", "Cancelamentos de BPE"),
      ("ADMDEC.DEC_DFE_CTE_SVD", "CT-e SVD (Conhecimento de Transporte Eletrônico)"),
      ("ADMDEC.DEC_DFE_CTE_SVD_CANCELAMENTO", "Cancelamentos de CT-e SVD"),
      ("ADMDEC.DEC_DFE_MDFE", "MDF-e (Manifesto de Documentos Fiscais Eletrônicos)"),
      ("ADMDEC.DEC_DFE_MDFE_CANCELAMENTO", "Cancelamentos de MDF-e"),
      ("ADMDEC.DEC_DFE_MDFE_EVENTO", "Eventos de MDF-e")
    )

    println(s"Gerando relatório de totalização para o período: $DATA_INICIO a $DATA_FIM")
    println("=" * 80)

    // DataFrame para armazenar os resultados
    var resultadosDF = Seq.empty[(String, String, Long)].toDF("tabela", "descricao", "quantidade")

    // Executar queries para cada tabela
    for ((tabela, descricao) <- tabelas) {
      try {
        val query = s"""
          SELECT COUNT(chave) as TOTAL
          FROM $tabela
          WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS')
          AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
        """

        println(s"Executando query para: $descricao")
        println(s"Query: ${query.take(100)}...")

        // Executar a query
        val countDF = spark.read.jdbc(jdbcUrl, s"($query) tmp", connectionProperties)

        // Obter o resultado - CORREÇÃO: usar "TOTAL" em maiúsculas
        val total = if (!countDF.isEmpty) {
          val row = countDF.first()
          // Verificar diferentes formas de obter o valor
          if (row.schema.fieldNames.contains("TOTAL")) {
            row.getAs[java.math.BigDecimal]("TOTAL").longValue()
          } else if (row.schema.fieldNames.contains("total")) {
            row.getAs[java.math.BigDecimal]("total").longValue()
          } else {
            // Tentar obter pelo índice se o nome não for encontrado
            row.getAs[java.math.BigDecimal](0).longValue()
          }
        } else {
          0L
        }

        // Adicionar ao DataFrame de resultados
        val novoResultado = Seq((tabela, descricao, total)).toDF("tabela", "descricao", "quantidade")
        resultadosDF = resultadosDF.union(novoResultado)

        println(s" → Total: $total registros")
      } catch {
        case e: Exception =>
          println(s" → Erro ao consultar $descricao: ${e.getMessage}")
          e.printStackTrace()
          // Adicionar registro com erro
          val erroResultado = Seq((tabela, s"$descricao (ERRO: ${e.getMessage.take(50)}...)", -1L))
            .toDF("tabela", "descricao", "quantidade")
          resultadosDF = resultadosDF.union(erroResultado)
      }
    }

    println("\n" + "=" * 80)
    println("RELATÓRIO CONSOLIDADO DE TOTALIZAÇÃO")
    println("=" * 80)

    // Calcular totais gerais - filtrar apenas valores positivos
    val resultadosValidos = resultadosDF.filter($"quantidade" >= 0)
    val totalGeral = if (!resultadosValidos.isEmpty) {
      resultadosValidos.agg(sum("quantidade")).first().getLong(0)
    } else {
      0L
    }

    val totalTabelas = resultadosDF.count()

    // ADIÇÃO: Remover o prefixo "ADMDEC.DEC_DFE_" da coluna tabela para exibição
    val resultadosExibicao = resultadosDF.withColumn(
      "tabela_simplificada",
      regexp_replace($"tabela", "ADMDEC\\.DEC_DFE_", "")
    ).select(
      $"tabela_simplificada".as("tabela"),
      $"descricao",
      $"quantidade"
    )

    // Exibir relatório formatado
    println("\nResultados detalhados (ordem alfabética de tabela):")
    resultadosExibicao.orderBy($"tabela".asc).show(truncate = false)

    println("\n" + "=" * 80)
    println(s"PERÍODO ANALISADO: $DATA_INICIO a $DATA_FIM")
    println(s"TOTAL DE TABELAS CONSULTADAS: $totalTabelas")
    println(s"TOTAL GERAL DE REGISTROS: $totalGeral")
    println("=" * 80)

    // Salvar o relatório em diferentes formatos
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = java.time.LocalDateTime.now().format(formatter)

    // Caminhos para salvar o relatório
    val hdfsBasePath = "/datalake/bronze/sources/dbms/dec/relatorios"
    // Usar as mesmas variáveis de data
    val dataInicioSemBarras = DATA_INICIO.replace("/", "")
    val dataFimSemBarras = DATA_FIM.replace("/", "")
    val csvPath = s"$hdfsBasePath/csv/totalizacao_${dataInicioSemBarras}_${dataFimSemBarras}_$timestamp"
    val parquetPath = s"$hdfsBasePath/parquet/totalizacao_${dataInicioSemBarras}_${dataFimSemBarras}_$timestamp"
    val jsonPath = s"$hdfsBasePath/json/totalizacao_${dataInicioSemBarras}_${dataFimSemBarras}_$timestamp"

    // ADIÇÃO: Criar versão simplificada para salvar (opcional - manter original também)
    val relatorioSimplificado = resultadosDF.withColumn(
      "tabela_simplificada",
      regexp_replace($"tabela", "ADMDEC\\.DEC_DFE_", "")
    ).withColumn(
      "tabela_original",
      $"tabela"  // Manter a tabela original também
    ).select(
      $"tabela_simplificada".as("tabela"),
      $"tabela_original",
      $"descricao",
      $"quantidade"
    )

    // Adicionar colunas de metadados ao relatório simplificado
    val relatorioCompleto = relatorioSimplificado
      .withColumn("data_inicio", lit(DATA_INICIO))
      .withColumn("data_fim", lit(DATA_FIM))
      .withColumn("data_geracao", current_timestamp())
      .withColumn("total_geral", lit(totalGeral))

    // Salvar em diferentes formatos
    println(s"\nSalvando relatório nos seguintes caminhos:")

    // CSV
    relatorioCompleto.coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ";")
      .mode("overwrite")
      .csv(csvPath)
    println(s" • CSV: $csvPath")

    // Parquet
    relatorioCompleto.write
      .mode("overwrite")
      .parquet(parquetPath)
    println(s" • Parquet: $parquetPath")

    // JSON
    relatorioCompleto.coalesce(1)
      .write
      .mode("overwrite")
      .json(jsonPath)
    println(s" • JSON: $jsonPath")

    // Gerar relatório resumido por categoria usando a tabela simplificada
    println("\n" + "=" * 80)
    println("RESUMO POR CATEGORIA DE DOCUMENTO")
    println("=" * 80)

    // Categorizar os documentos
    val relatorioCategorizado = relatorioSimplificado
      .withColumn("categoria", when($"descricao".contains("NF-e"), "NF-e")
        .when($"descricao".contains("NFC-e"), "NFC-e")
        .when($"descricao".contains("NF3-e"), "NF3-e")
        .when($"descricao".contains("NFCom"), "NFCom")
        .when($"descricao".contains("MDF-e"), "MDF-e")
        .when($"descricao".contains("CT-e"), "CT-e")
        .when($"descricao".contains("BPE"), "BPE")
        .otherwise("Outros")
      )
      .groupBy("categoria")
      .agg(
        sum("quantidade").as("total_categoria"),
        count("*").as("qtd_tabelas")
      )
      .orderBy($"total_categoria".desc)

    relatorioCategorizado.show(truncate = false)

    // Salvar o relatório categorizado
    val categoriaPath = s"$hdfsBasePath/parquet/resumo_categorias_${dataInicioSemBarras}_${dataFimSemBarras}_$timestamp"
    relatorioCategorizado.write
      .mode("overwrite")
      .parquet(categoriaPath)
    println(s"\nRelatório categorizado salvo em: $categoriaPath")

    // Criar visualização SQL para consulta fácil
    relatorioCompleto.createOrReplaceTempView("relatorio_totalizacao")
    relatorioCategorizado.createOrReplaceTempView("resumo_categorias")

    println("\n" + "=" * 80)
    println("RELATÓRIO FINALIZADO COM SUCESSO!")
    println("=" * 80)

    // Exemplos de consultas que podem ser feitas
    println("\nExemplos de consultas disponíveis:")
    println(" • spark.sql(\"SELECT tabela, descricao, quantidade FROM relatorio_totalizacao WHERE quantidade > 0 ORDER BY quantidade DESC\").show()")
    println(" • spark.sql(\"SELECT categoria, SUM(total_categoria) as total FROM resumo_categorias GROUP BY categoria\").show()")

    spark.stop()
  }
}

// Versão alternativa que verifica quais tabelas existem antes de consultar
object RelatorioTotalizacaoDFEComValidacao {
  // Usa as mesmas datas do objeto principal para garantir consistência
  val DATA_INICIO = RelatorioTotalizacaoDFE.DATA_INICIO
  val DATA_FIM = RelatorioTotalizacaoDFE.DATA_FIM

  def verificarTabelaExiste(spark: SparkSession, jdbcUrl: String, properties: Properties, tabela: String): Boolean = {
    try {
      val query = s"SELECT 1 FROM $tabela WHERE 1=0"
      spark.read.jdbc(jdbcUrl, s"($query) tmp", properties)
      true
    } catch {
      case e: Exception => false
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RelatorioTotalizacaoDFEComValidacao")
      .config("spark.yarn.queue", "workloads")
      .getOrCreate()

    import spark.implicits._

    // Configurações de conexão
    val jdbcUrl = "jdbc:oracle:thin:@codvm01-scan1.gdfnet.df:1521/ORAPRD23"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admhadoop")
    connectionProperties.put("password", ".admhadoop#")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    // Formatar as datas para o padrão do Oracle
    val dataInicial = s"$DATA_INICIO 00:00:00"
    val dataFinal = s"$DATA_FIM 23:59:59"

    // Lista de possíveis tabelas
    val possiveisTabelas = Seq(
      "ADMDEC.DEC_DFE_NFE",
      "ADMDEC.DEC_DFE_NFE_CANCELAMENTO",
      "ADMDEC.DEC_DFE_NFE_EVENTO",
      "ADMDEC.DEC_DFE_EVENTO", // Tentar esta também caso a anterior não exista
      "ADMDEC.DEC_DFE_NFCE",
      "ADMDEC.DEC_DFE_NFCE_CANCELAMENTO",
      "ADMDEC.DEC_DFE_NF3E",
      "ADMDEC.DEC_DFE_NF3E_CANCELAMENTO",
      "ADMDEC.DEC_DFE_NF3E_EVENTO",
      "ADMDEC.DEC_DFE_NFCOM",
      "ADMDEC.DEC_DFE_NFCOM_CANCELAMENTO",
      "ADMDEC.DEC_DFE_NFCOM_EVENTO",
      "ADMDEC.DEC_DFE_BPE",
      "ADMDEC.DEC_DFE_BPE_CANCELAMENTO",
      "ADMDEC.DEC_DFE_CTE_SVD",
      "ADMDEC.DEC_DFE_CTE_SVD_CANCELAMENTO",
      "ADMDEC.DEC_DFE_MDFE",
      "ADMDEC.DEC_DFE_MDFE_CANCELAMENTO",
      "ADMDEC.DEC_DFE_MDFE_EVENTO"
    )

    val descricoes = Map(
      "ADMDEC.DEC_DFE_NFE" -> "NF-e (Notas Fiscais Eletrônicas)",
      "ADMDEC.DEC_DFE_NFE_CANCELAMENTO" -> "Cancelamentos de NF-e",
      "ADMDEC.DEC_DFE_NFE_EVENTO" -> "Eventos de NF-e (exceto cancelamentos)",
      "ADMDEC.DEC_DFE_EVENTO" -> "Eventos de NF-e (tabela genérica)",
      "ADMDEC.DEC_DFE_NFCE" -> "NFC-e (Notas Fiscais de Consumidor Eletrônicas)",
      "ADMDEC.DEC_DFE_NFCE_CANCELAMENTO" -> "Cancelamentos de NFC-e",
      "ADMDEC.DEC_DFE_NF3E" -> "NF3-e (Notas Fiscais de Energia Elétrica)",
      "ADMDEC.DEC_DFE_NF3E_CANCELAMENTO" -> "Cancelamentos de NF3-e",
      "ADMDEC.DEC_DFE_NF3E_EVENTO" -> "Eventos de NF3-e",
      "ADMDEC.DEC_DFE_NFCOM" -> "NFCom (Notas Fiscais de Comunicação)",
      "ADMDEC.DEC_DFE_NFCOM_CANCELAMENTO" -> "Cancelamentos de NFCom",
      "ADMDEC.DEC_DFE_NFCOM_EVENTO" -> "Eventos de NFCom",
      "ADMDEC.DEC_DFE_BPE" -> "BPE (Bilhetes de Passagem Eletrônicos)",
      "ADMDEC.DEC_DFE_BPE_CANCELAMENTO" -> "Cancelamentos de BPE",
      "ADMDEC.DEC_DFE_CTE_SVD" -> "CT-e SVD (Conhecimento de Transporte Eletrônico)",
      "ADMDEC.DEC_DFE_CTE_SVD_CANCELAMENTO" -> "Cancelamentos de CT-e SVD",
      "ADMDEC.DEC_DFE_MDFE" -> "MDF-e (Manifesto de Documentos Fiscais Eletrônicos)",
      "ADMDEC.DEC_DFE_MDFE_CANCELAMENTO" -> "Cancelamentos de MDF-e",
      "ADMDEC.DEC_DFE_MDFE_EVENTO" -> "Eventos de MDF-e"
    )

    println(s"Verificando tabelas disponíveis no banco de dados para o período: $DATA_INICIO a $DATA_FIM")
    println("=" * 80)

    var resultadosDF = Seq.empty[(String, String, Long)].toDF("tabela", "descricao", "quantidade")

    // Primeiro verificar quais tabelas existem
    val tabelasExistentes = possiveisTabelas.filter { tabela =>
      val existe = verificarTabelaExiste(spark, jdbcUrl, connectionProperties, tabela)
      if (existe) {
        println(s"✓ Tabela encontrada: $tabela")
      }
      existe
    }

    println(s"\nTotal de tabelas encontradas: ${tabelasExistentes.size}")
    println("=" * 80)

    // Agora consultar apenas as tabelas que existem
    for (tabela <- tabelasExistentes) {
      val descricao = descricoes.getOrElse(tabela, tabela)
      try {
        val query = s"""
          SELECT COUNT(chave) as TOTAL
          FROM $tabela
          WHERE DHPROC BETWEEN TO_DATE('$dataInicial', 'DD/MM/YYYY HH24:MI:SS')
          AND TO_DATE('$dataFinal', 'DD/MM/YYYY HH24:MI:SS')
        """

        println(s"\nConsultando: $descricao")

        val countDF = spark.read.jdbc(jdbcUrl, s"($query) tmp", connectionProperties)

        val total = if (!countDF.isEmpty) {
          val row = countDF.first()
          // Método mais robusto para obter o valor
          try {
            row.getAs[java.math.BigDecimal]("TOTAL").longValue()
          } catch {
            case _: Exception =>
              try {
                row.getAs[java.math.BigDecimal](0).longValue()
              } catch {
                case _: Exception => 0L
              }
          }
        } else {
          0L
        }

        val novoResultado = Seq((tabela, descricao, total)).toDF("tabela", "descricao", "quantidade")
        resultadosDF = resultadosDF.union(novoResultado)

        println(s" → Total encontrado: $total registros")
      } catch {
        case e: Exception =>
          println(s" → Erro na consulta: ${e.getMessage}")
      }
    }

    // ADIÇÃO: Remover o prefixo "ADMDEC.DEC_DFE_" da coluna tabela para exibição
    val resultadosExibicao = resultadosDF.withColumn(
      "tabela_simplificada",
      regexp_replace($"tabela", "ADMDEC\\.DEC_DFE_", "")
    ).select(
      $"tabela_simplificada".as("tabela"),
      $"descricao",
      $"quantidade"
    )

    // Exibir resultados
    println("\n" + "=" * 80)
    println("RELATÓRIO FINAL")
    println("=" * 80)
    resultadosExibicao.orderBy($"tabela".asc).show(truncate = false)

    spark.stop()
  }
}

//RelatorioTotalizacaoDFE.main(Array())