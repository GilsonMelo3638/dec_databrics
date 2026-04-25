package RepartitionJob

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, xxhash64}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.round

object RepartitionXlmPequenosMediosProcessor {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  // 🎯 parâmetros ideais
  val TARGET_MB = 160.0
  val MAX_FILE_MB = 256.0
  val MAX_RECORDS_PER_FILE = 4200000

  // 🧠 zona de estabilidade (anti-loop REAL)
  val LOWER_TOLERANCE = 0.85
  val UPPER_TOLERANCE = 1.15

  def main(args: Array[String]): Unit = {

    spark.conf.set("spark.sql.files.maxRecordsPerFile", MAX_RECORDS_PER_FILE)

    val basePaths = Seq(
      "/datalake/prata/sources/dbms/dec/nf3e/cancelamento",
      ("/datalake/prata/sources/dbms/dec/cte/CTe"),
      ("/datalake/prata/sources/dbms/dec/cte/CTeOS"),
      ("/datalake/prata/sources/dbms/dec/cte/GVTe"),
      ("/datalake/prata/sources/dbms/dec/bpe/BPe"),
      ("/datalake/prata/sources/dbms/dec/mdfe/MDFe"),
      ("/datalake/prata/sources/dbms/dec/nf3e/NF3e"),
      ("/datalake/prata/sources/dbms/dec/nfcom/NFCom")
    )
    basePaths.foreach { path =>
      println(s"\n🚀 Processando: $path")
      processPartitions(path)
    }

    println("\n✅ Finalizado.")
  }

  def processPartitions(basePath: String): Unit = {

    val partitions = fs.listStatus(new Path(basePath))
      .filter(_.isDirectory)
      .map(_.getPath.toString)

    partitions.foreach(processSinglePartition)
  }

  def processSinglePartition(partitionPath: String): Unit = {

    try {

      val path = new Path(partitionPath)

      val parquetFiles = fs.listStatus(path)
        .filter(f => f.isFile && f.getPath.getName.endsWith(".parquet"))

      val fileCount = parquetFiles.length

      val sizeMB = getDirectorySize(path) / (1024.0 * 1024.0)

      // 🎯 cálculo ESTÁVEL (sem ceil)
      val basePartitions = round(sizeMB / TARGET_MB).toInt

      // 🔒 garante limite de tamanho máximo
      val minPartitions = round(sizeMB / MAX_FILE_MB).toInt

      val idealPartitions = Seq(basePartitions, minPartitions, 1).max

      // 🧠 faixa aceitável
      val lowerBound = (idealPartitions * LOWER_TOLERANCE).toInt
      val upperBound = (idealPartitions * UPPER_TOLERANCE).toInt

      logPartition(partitionPath, sizeMB, fileCount, idealPartitions, lowerBound, upperBound)

      val isHealthy =
        fileCount >= lowerBound &&
          fileCount <= upperBound

      if (isHealthy) {
        println("✔ Já está saudável. Pulando.")
        return
      }

      println("⚡ Reorganizando...")

      val df = spark.read.parquet(partitionPath)

      val repartitionedDF = rebalance(df, idealPartitions)

      val tempPath = s"${partitionPath}_tmp"

      repartitionedDF
        .write
        .mode("overwrite")
        .option("compression", "lz4")
        .parquet(tempPath)

      fs.delete(path, true)
      fs.rename(new Path(tempPath), path)

      println("✅ Concluído")

    } catch {
      case e: Exception =>
        System.err.println(s"❌ Erro em $partitionPath: ${e.getMessage}")
    }
  }

  // =========================
  // DISTRIBUIÇÃO INTELIGENTE
  // =========================

  def rebalance(df: DataFrame, numPartitions: Int): DataFrame = {

    val currentPartitions = df.rdd.getNumPartitions

    // 🚫 evita shuffle inútil
    if (currentPartitions == numPartitions) {
      println("✔ Mesmo número de partições. Mantendo.")
      return df
    }

    val hasChave = df.columns.contains("CHAVE")

    if (hasChave) {
      println("🔑 Usando CHAVE")
      df.withColumn("_hash", xxhash64(col("CHAVE")))
        .repartition(numPartitions, col("_hash"))
        .drop("_hash")
    } else {
      println("⚠️ Fallback (todas colunas)")
      df.withColumn("_hash", xxhash64(df.columns.map(col): _*))
        .repartition(numPartitions, col("_hash"))
        .drop("_hash")
    }
  }

  // =========================
  // UTILS
  // =========================

  def getDirectorySize(path: Path): Long = {
    fs.listStatus(path).map { status =>
      if (status.isFile) status.getLen
      else getDirectorySize(status.getPath)
    }.sum
  }

  def logPartition(path: String,
                   sizeMB: Double,
                   files: Int,
                   ideal: Int,
                   lower: Int,
                   upper: Int): Unit = {

    println(s"\n📂 $path")
    println(f"📦 Tamanho: $sizeMB%.2f MB")
    println(s"📄 Arquivos atuais: $files")
    println(s"🎯 Ideal: $ideal")
    println(s"📏 Faixa aceitável: [$lower - $upper]")
  }
}


//RepartitionXlmPequenosMediosProcessor.main(Array())