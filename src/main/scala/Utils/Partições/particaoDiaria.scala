package Utils.Particições
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object particaoDiaria {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NFE Partitioning Final")
      .config("spark.sql.parquet.compression.codec", "lz4")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    try {
      val basePath = "/datalake/bronze/sources/dbms/dec/"
      val inputBase = s"${basePath}nfce/"
      val outputPath = s"${basePath}nfce_diario/"

      // Parâmetros flexíveis para o período de processamento
      val startYear = 2025
      val endYear = 2025
      val startMonth = 3
      val endMonth = 3

      processPeriod(spark, inputBase, outputPath, startYear, endYear, startMonth, endMonth)
      println("✅ Processamento concluído com sucesso!")
    } finally {
      spark.stop()
    }
  }

  private def processPeriod(
                             spark: SparkSession,
                             inputBase: String,
                             outputPath: String,
                             startYear: Int,
                             endYear: Int,
                             startMonth: Int,
                             endMonth: Int
                           ): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    (startYear to endYear).foreach { year =>
      // Determinar os meses a serem processados para cada ano
      val (firstMonth, lastMonth) = if (year == startYear && year == endYear) {
        (startMonth, endMonth)
      } else if (year == startYear) {
        (startMonth, 12)
      } else if (year == endYear) {
        (1, endMonth)
      } else {
        (1, 12)
      }

      (firstMonth to lastMonth).foreach { month =>
        val monthStr = f"$year$month%02d"
        val inputPath = s"$inputBase$monthStr/"

        if (fs.exists(new Path(inputPath))) {
          try {
            processSingleMonth(spark, inputPath, outputPath)
            println(s"✔ $monthStr processado com sucesso")
          } catch {
            case e: Exception => println(s"❌ Erro ao processar $monthStr: ${e.getMessage}")
          }
        } else {
          println(s"⚠️ Pasta $monthStr não encontrada")
        }
      }
    }
  }

  private def processSingleMonth(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val df = spark.read.parquet(inputPath)

    val partitionedDF = df.withColumn("year", substring(col("DHPROC"), 7, 4))
      .withColumn("month", substring(col("DHPROC"), 4, 2))
      .withColumn("day", substring(col("DHPROC"), 1, 2))

    // Obter lista de dias únicos
    val days = partitionedDF.select("year", "month", "day").distinct().collect()

    days.foreach { day =>
      val year = day.getString(0)
      val month = day.getString(1)
      val dayNum = day.getString(2)

      val dayDF = partitionedDF.filter(col("year") === year &&
        col("month") === month &&
        col("day") === dayNum)
        .drop("year", "month", "day")

      // Número fixo de partições baseado no tamanho esperado
      val numPartitions = 4 // Para ~5GB/dia resulta em ~500MB/arquivo

      dayDF.repartition(numPartitions)
        .write
        .mode("append")
        .option("compression", "lz4")
        .parquet(s"$outputPath/year=$year/month=$month/day=$dayNum")
    }
  }
}

// Execução
//particaoDiaria.main(Array())