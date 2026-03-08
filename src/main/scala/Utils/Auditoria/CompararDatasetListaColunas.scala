package Utils.Auditoria

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class CompararDatasetListaColunas(spark: SparkSession) {

  /**
   * Carrega parquet e retorna apenas colunas distintas
   */
  private def loadDistinct(path: String, columns: Seq[String]): DataFrame = {
    spark.read
      .parquet(path)
      .select(columns.map(c => col(c)): _*)   // <-- corrigido aqui
      .distinct()
  }

  /**
   * Retorna registros que estão no datasetA e NÃO estão no datasetB
   */
  def leftAnti(
                pathA: String,
                pathB: String,
                columns: Seq[String],
                showMetrics: Boolean = true
              ): DataFrame = {

    val dfA = loadDistinct(pathA, columns)
    val dfB = loadDistinct(pathB, columns)

    val result = dfA.join(dfB, columns, "left_anti")

    if (showMetrics) {
      println(s"Total distinto em A: ${dfA.count()}")
      println(s"Total distinto em B: ${dfB.count()}")
      println(s"Presentes em A e NÃO em B: ${result.count()}")
    }

    result
  }
}

//val comparator = new CompararDatasetListaColunas(spark)
//
//val pathIBS = "/datalake/prata/sources/dbms/dec/nfe/det"
//val pathDet = "/datalake/prata/backup_producao/nfe/det"
//
//val dfIBSNotInDet = comparator.leftAnti(
//  pathA = pathIBS,
//  pathB = pathDet,
//  columns = Seq("CHAVE", "nitem")
//)
//
//dfIBSNotInDet.show(20, false)
