package RepartitionJob

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, xxhash64}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Calendar
import scala.math.ceil

object RepartitionProcessor {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val TARGET_MB = 256.0
  val MAX_RECORDS_PER_FILE = 5000000

  val TOLERANCE_PERCENT = 0.3
  val TOLERANCE_ABSOLUTE = 1

  def main(args: Array[String]): Unit = {

    spark.conf.set("spark.sql.files.maxRecordsPerFile", MAX_RECORDS_PER_FILE)

    val calendar = Calendar.getInstance()
    val isSunday = calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY

    val configs = Seq(
      ("/datalake/prata/sources/dbms/dec/nfe/infNFe", false),
      ("/datalake/prata/sources/dbms/dec/nfe/det", false),
      ("/datalake/prata/sources/dbms/dec/nfce/infNFCe", false),
      ("/datalake/prata/sources/dbms/dec/nfce/det", true) // diretório especial

    )

    configs.foreach { case (path, isNfceDet) =>
      println(s"\n🚀 Processando: $path")
      processPartitions(path, isNfceDet, isSunday)
    }

    println("\n✅ Finalizado.")
  }

  def processPartitions(basePath: String, isNfceDet: Boolean, isSunday: Boolean): Unit = {

    val partitions = fs.listStatus(new Path(basePath))
      .filter(_.isDirectory)
      .map(_.getPath.toString)

    val sortedPartitions =
      partitions
        .map(p => (p.split("=").last.toInt, p))
        .sortBy(_._1)
        .map(_._2)

    val partitionsToProcess =
      if (isNfceDet && !isSunday) sortedPartitions.takeRight(2)
      else sortedPartitions

    partitionsToProcess.foreach(processSinglePartition)
  }

  def processSinglePartition(partitionPath: String): Unit = {

    try {

      val path = new Path(partitionPath)

      val parquetFiles = fs.listStatus(path)
        .filter(f => f.isFile && f.getPath.getName.endsWith(".parquet"))

      val fileCount = parquetFiles.length

      val sizeBytes = getDirectorySize(path)
      val sizeMB = sizeBytes / (1024.0 * 1024.0)

      val idealPartitions = math.max(1, ceil(sizeMB / TARGET_MB).toInt)

      val diff = math.abs(fileCount - idealPartitions)
      val percentDiff =
        if (idealPartitions == 0) 0.0
        else diff.toDouble / idealPartitions.toDouble

      logPartition(partitionPath, sizeMB, fileCount, idealPartitions, percentDiff)

      val shouldRepartition =
        diff > TOLERANCE_ABSOLUTE &&
          percentDiff > TOLERANCE_PERCENT

      if (!shouldRepartition) {
        println("✔ Já está otimizado. Pulando.")
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

    if (currentPartitions == numPartitions) {
      println("✔ Mesmo número de partições. Mantendo.")
      return df
    }

    val hasChave = df.columns.contains("CHAVE")

    if (hasChave) {
      println("🔑 Usando CHAVE para distribuição determinística")

      df.withColumn("_hash", xxhash64(col("CHAVE")))
        .repartition(numPartitions, col("_hash"))
        .drop("_hash")

    } else {
      println("⚠️ CHAVE não encontrada. Usando fallback (todas colunas)")

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

  def logPartition(path: String, sizeMB: Double, files: Int, ideal: Int, diff: Double): Unit = {
    println(s"\n📂 $path")
    println(f"📦 Tamanho: $sizeMB%.2f MB")
    println(s"📄 Arquivos atuais: $files")
    println(s"🎯 Ideal: $ideal")
    println(f"📊 Diferença: ${diff * 100}%.2f%%")
  }
}

//RepartitionProcessor.main(Array())