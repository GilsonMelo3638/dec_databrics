package Utils.Auditoria

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CompareSchemas {

  case class FieldDifference(
                              path: String,
                              difference: String,
                              schema1: String,
                              schema2: String
                            )

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {

      println(
        """
          |======================================================================
          | USO
          |======================================================================
          |
          |spark-submit ... Utils.CompareSchemas <path1> <path2>
          |
          |Exemplo:
          |
          |spark-submit ... Utils.CompareSchemas \
          |  /datalake/prata/sources/dbms/dec/nfcom/NFCom \
          |  /datalake/prata/sources/dbms/dec/nfcom/NFCom2
          |
          |======================================================================
          |""".stripMargin
      )

      System.exit(1)
    }

    val path1 = args(0)
    val path2 = args(1)

    val spark = SparkSession.builder()
      .appName("CompareSchemas")
      .getOrCreate()

    try {

      println()
      println("=" * 100)
      println("COMPARAÇÃO DE DATAFRAMES")
      println("=" * 100)

      println(s"Schema 1: $path1")
      println(s"Schema 2: $path2")

      // --------------------------------------------------------------------
      // Leitura dos DataFrames
      // --------------------------------------------------------------------

      val df1 = spark.read.parquet(path1)
      val df2 = spark.read.parquet(path2)

      // --------------------------------------------------------------------
      // Informações gerais
      // --------------------------------------------------------------------

      val total1 = df1.count()
      val total2 = df2.count()

      val distinctNSUCount1 = countDistinctNSU(df1)
      val distinctNSUCount2 = countDistinctNSU(df2)

      val fieldCount1 = countLeafFields(df1.schema)
      val fieldCount2 = countLeafFields(df2.schema)

      // --------------------------------------------------------------------
      // 1 - QUANTIDADE DE REGISTROS
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("1 - QUANTIDADE DE REGISTROS")
      println("=" * 100)

      println(f"Schema 1: $total1%,d registros")
      println(f"Schema 2: $total2%,d registros")

      if (total1 == total2) {

        println("RESULTADO: OK - Quantidade de registros igual")

      } else {

        println(
          s"RESULTADO: DIFERENTE - Diferença de ${math.abs(total1 - total2)} registro(s)"
        )
      }

      // --------------------------------------------------------------------
      // 2 - NSUs DISTINTOS
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("2 - NSUs DISTINTOS")
      println("=" * 100)

      println(f"Schema 1: $distinctNSUCount1%,d NSUs distintos")
      println(f"Schema 2: $distinctNSUCount2%,d NSUs distintos")

      if (distinctNSUCount1 == distinctNSUCount2) {

        println("RESULTADO: OK - Quantidade de NSUs distintos igual")

      } else {

        println(
          s"RESULTADO: DIFERENTE - Diferença de ${math.abs(distinctNSUCount1 - distinctNSUCount2)} NSU(s)"
        )
      }

      // --------------------------------------------------------------------
      // 3 - QUANTIDADE DE CAMPOS
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("3 - QUANTIDADE DE CAMPOS")
      println("=" * 100)

      println(s"Schema 1: $fieldCount1 campos")
      println(s"Schema 2: $fieldCount2 campos")

      if (fieldCount1 == fieldCount2) {

        println("RESULTADO: OK - Quantidade de campos igual")

      } else {

        println(
          s"RESULTADO: DIFERENTE - Diferença de ${math.abs(fieldCount1 - fieldCount2)} campo(s)"
        )
      }

      // --------------------------------------------------------------------
      // 4 - COMPARAÇÃO DO SCHEMA
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("4 - COMPARAÇÃO DO SCHEMA")
      println("=" * 100)

      val differences = compareSchemas(
        df1.schema,
        df2.schema
      )

      if (differences.isEmpty) {

        println("RESULTADO: OK - Os schemas são iguais")

      } else {

        println(
          s"RESULTADO: DIFERENTE - ${differences.size} diferença(s) encontrada(s)"
        )

        println()
        println("DETALHAMENTO DAS DIFERENÇAS")
        println("-" * 100)

        differences.foreach { diff =>

          println()
          println(s"Campo    : ${diff.path}")
          println(s"Diferença: ${diff.difference}")
          println(s"Schema 1 : ${diff.schema1}")
          println(s"Schema 2 : ${diff.schema2}")
        }
      }

      // --------------------------------------------------------------------
      // 5 - SCHEMA 1
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("5 - SCHEMA 1")
      println("=" * 100)

      df1.printSchema()

      // --------------------------------------------------------------------
      // 6 - SCHEMA 2
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("6 - SCHEMA 2")
      println("=" * 100)

      df2.printSchema()

      // --------------------------------------------------------------------
      // 7 - AMOSTRA DOS DADOS
      // --------------------------------------------------------------------

      println()
      println("=" * 100)
      println("7 - AMOSTRA DOS DADOS")
      println("=" * 100)

      println()
      println("Schema 1:")
      df1.show(false)

      println()
      println("Schema 2:")
      df2.show(false)

      // --------------------------------------------------------------------
      // RESULTADO FINAL
      // --------------------------------------------------------------------

      val registrosOk = total1 == total2
      val nsusOk = distinctNSUCount1 == distinctNSUCount2
      val camposOk = fieldCount1 == fieldCount2
      val schemaOk = differences.isEmpty

      println()
      println("=" * 100)
      println("RESULTADO FINAL")
      println("=" * 100)

      println(
        f"Quantidade de registros : ${status(registrosOk)}"
      )

      println(
        f"Quantidade de NSUs      : ${status(nsusOk)}"
      )

      println(
        f"Quantidade de campos    : ${status(camposOk)}"
      )

      println(
        f"Schema                  : ${status(schemaOk)}"
      )

      println()

      if (
        registrosOk &&
          nsusOk &&
          camposOk &&
          schemaOk
      ) {

        println("============================================================")
        println("RESULTADO: OK")
        println("Os DataFrames possuem a mesma quantidade de registros,")
        println("mesma quantidade de NSUs distintos, mesma quantidade")
        println("de campos e schemas compatíveis.")
        println("============================================================")

      } else {

        println("============================================================")
        println("RESULTADO: DIFERENÇAS ENCONTRADAS")
        println("É necessário analisar os itens apresentados acima.")
        println("============================================================")
      }

    } finally {

      spark.stop()
    }
  }

  // ==========================================================================
  // CONTAGEM DE NSU
  // ==========================================================================

  /**
  * Conta os NSUs distintos.
    *
  * A procura pelo campo é feita ignorando maiúsculas/minúsculas.
    *
  * Assim, os seguintes nomes são considerados equivalentes:
    *
  * NSU
    * nsu
    * Nsu
    * NsU
    */
  private def countDistinctNSU(df: DataFrame): Long = {

    val nsuColumn = df.columns.find(
      _.equalsIgnoreCase("nsu")
    )

    nsuColumn match {

      case Some(columnName) =>

        df.select(columnName)
          .distinct()
          .count()

      case None =>

        println(
          "ATENÇÃO: Campo 'NSU' não encontrado."
        )

        0L
    }
  }

  // ==========================================================================
  // CONTAGEM DE CAMPOS FOLHA
  // ==========================================================================

  /**
   * Conta somente os campos folha do schema.
   *
   * Exemplo:
   *
   * Struct
   *   campo1: String
   *   campo2: Struct
   *     campo3: String
   *
   * Resultado:
   *
   * 2 campos folha
   *
   * StructType e ArrayType não são contabilizados como
   * campos de dados.
   */
  private def countLeafFields(
                               dataType: DataType
                             ): Int = {

    dataType match {

      case StructType(fields) =>

        fields
          .map(field => countLeafFields(field.dataType))
          .sum

      case ArrayType(elementType, _) =>

        countLeafFields(elementType)

      case MapType(keyType, valueType, _) =>

        countLeafFields(keyType) +
          countLeafFields(valueType)

      case _ =>

        1
    }
  }

  // ==========================================================================
  // COMPARAÇÃO DOS SCHEMAS
  // ==========================================================================

  /**
  * Compara os dois schemas recursivamente.
  *
  * A comparação dos nomes dos campos ignora maiúsculas/minúsculas.
    *
  * Exemplos considerados iguais:
    *
  * NSU
    * nsu
    *
  * DHEMI
    * dhemi
    */
  private def compareSchemas(
                              schema1: StructType,
                              schema2: StructType
                            ): Seq[FieldDifference] = {

    compareDataTypes(
      schema1,
      schema2,
      ""
    )
  }

  // ==========================================================================
  // COMPARAÇÃO RECURSIVA DOS TIPOS
  // ==========================================================================

  private def compareDataTypes(
                                type1: DataType,
                                type2: DataType,
                                path: String
                              ): Seq[FieldDifference] = {

    (type1, type2) match {

      // ======================================================================
      // STRUCT
      // ======================================================================

      case (
        StructType(fields1),
        StructType(fields2)
        ) =>

        val differences = scala.collection.mutable.ArrayBuffer.empty[
          FieldDifference
        ]

        // --------------------------------------------------------------
        // Campos existentes nos dois schemas
        // --------------------------------------------------------------

        fields1.foreach { field1 =>

          val field2Option = fields2.find(
            _.name.equalsIgnoreCase(field1.name)
          )

          field2Option match {

            case Some(field2) =>

              val fieldPath =
                appendPath(
                  path,
                  field1.name
                )

              // --------------------------------------------------------
              // Verifica nullable
              // --------------------------------------------------------

              if (field1.nullable != field2.nullable) {

                differences += FieldDifference(
                  path = fieldPath,
                  difference = "Diferença de nullable",
                  schema1 = field1.nullable.toString,
                  schema2 = field2.nullable.toString
                )
              }

              // --------------------------------------------------------
              // Compara recursivamente o tipo
              // --------------------------------------------------------

              differences ++=
                compareDataTypes(
                  field1.dataType,
                  field2.dataType,
                  fieldPath
                )

            case None =>

              // --------------------------------------------------------
              // Campo existe somente no Schema 1
              // --------------------------------------------------------

              differences += FieldDifference(
                path = appendPath(
                  path,
                  field1.name
                ),
                difference = "Campo existe somente no Schema 1",
                schema1 = field1.dataType.simpleString,
                schema2 = "-"
              )
          }
        }

        // --------------------------------------------------------------
        // Procura campos existentes somente no Schema 2
        // --------------------------------------------------------------

        fields2.foreach { field2 =>

          val existsInSchema1 =
            fields1.exists(
              _.name.equalsIgnoreCase(field2.name)
            )

          if (!existsInSchema1) {

            differences += FieldDifference(
              path = appendPath(
                path,
                field2.name
              ),
              difference = "Campo existe somente no Schema 2",
              schema1 = "-",
              schema2 = field2.dataType.simpleString
            )
          }
        }

        differences.toSeq

      // ======================================================================
      // ARRAY
      // ======================================================================

      case (
        ArrayType(
        elementType1,
        containsNull1
        ),
        ArrayType(
        elementType2,
        containsNull2
        )
        ) =>

        val differences =
          scala.collection.mutable.ArrayBuffer.empty[
            FieldDifference
          ]

        // --------------------------------------------------------------
        // Verifica containsNull
        // --------------------------------------------------------------

        if (containsNull1 != containsNull2) {

          differences += FieldDifference(
            path = path,
            difference = "Diferença de containsNull",
            schema1 = containsNull1.toString,
            schema2 = containsNull2.toString
          )
        }

        // --------------------------------------------------------------
        // Compara o conteúdo do array
        // --------------------------------------------------------------

        differences ++=
          compareDataTypes(
            elementType1,
            elementType2,
            path + "[]"
          )

        differences.toSeq

      // ======================================================================
      // MAP
      // ======================================================================

      case (
        MapType(
        keyType1,
        valueType1,
        valueContainsNull1
        ),
        MapType(
        keyType2,
        valueType2,
        valueContainsNull2
        )
        ) =>

        val differences =
          scala.collection.mutable.ArrayBuffer.empty[
            FieldDifference
          ]

        // --------------------------------------------------------------
        // Compara chave
        // --------------------------------------------------------------

        differences ++=
          compareDataTypes(
            keyType1,
            keyType2,
            path + ".<key>"
          )

        // --------------------------------------------------------------
        // Compara valor
        // --------------------------------------------------------------

        differences ++=
          compareDataTypes(
            valueType1,
            valueType2,
            path + ".<value>"
          )

        // --------------------------------------------------------------
        // Verifica nullable do valor
        // --------------------------------------------------------------

        if (valueContainsNull1 != valueContainsNull2) {

          differences += FieldDifference(
            path = path,
            difference = "Diferença de valueContainsNull",
            schema1 = valueContainsNull1.toString,
            schema2 = valueContainsNull2.toString
          )
        }

        differences.toSeq

      // ======================================================================
      // TIPOS DIFERENTES
      // ======================================================================

      case _ if type1 != type2 =>

        Seq(
          FieldDifference(
            path = path,
            difference = "Tipos diferentes",
            schema1 = type1.simpleString,
            schema2 = type2.simpleString
          )
        )

      // ======================================================================
      // TIPOS IGUAIS
      // ======================================================================

      case _ =>

        Seq.empty
    }
  }

  // ==========================================================================
  // MONTA CAMINHO COMPLETO DO CAMPO
  // ==========================================================================

  private def appendPath(
                          parent: String,
                          child: String
                        ): String = {

    if (parent.isEmpty) {

      child

    } else {

      s"$parent.$child"
    }
  }

  // ==========================================================================
  // STATUS
  // ==========================================================================

  private def status(
                      ok: Boolean
                    ): String = {

    if (ok) {

      "OK"

    } else {

      "DIFERENTE"
    }
  }
}

//CompareSchemas.main(Array( "/datalake/prata/sources/dbms/dec/nfcom/NFCom", "/datalake/prata/sources/dbms/dec/nfcom/NFCom2"))