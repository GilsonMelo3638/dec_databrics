package Utils.Backup

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}
import java.time.{LocalDate, ZoneId}

object HDFSDirectorySync {
  def main(args: Array[String]): Unit = {
    // Configurações básicas
    val sourceRoot = "/datalake/prata/sources/dbms/dec"
    val destRoot = "/datalake/prata/backup_producao"
    val referenceDate = LocalDate.of(2025, 4, 6) // Data de referência para sincronização

    // Todos os diretórios que precisam ser sincronizados
    val directoriesToSync = Map(
      "bpe" -> List("BPe", "cancelamento"),
      "cte" -> List("CTe",  "CTeOS", "CTeSimp", "GVTe", "cancelamento"),
      "mdfe" -> List("MDFe", "cancelamento"),
      "nf3e" -> List("NF3e", "cancelamento"),
      "nfe" -> List("infNFe", "det", "cancelamento"),
      "nfce" -> List("infNFCe", "det", "cancelamento")
    )

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val spark = SparkSession.builder().appName("HDFS Backup Directory Sync").getOrCreate()

    try {
      // Processa cada categoria e seus subdiretórios
      directoriesToSync.foreach { case (mainDir, subDirs) =>
        println(s"\n=== PROCESSANDO $mainDir ===")

        subDirs.foreach { subDir =>
          val sourcePath = new Path(s"$sourceRoot/$mainDir/$subDir")
          val destPath = new Path(s"$destRoot/$mainDir/$subDir")

          if (fs.exists(sourcePath)) {
            println(s"\nSincronizando: $mainDir/$subDir")
            syncDirectory(fs, sourcePath, destPath, referenceDate)
            verifyCount(spark, sourcePath.toString, destPath.toString)
          } else {
            println(s"\nAviso: Diretório não encontrado - ${sourcePath.toString}")
          }
        }
      }

      println("\nSincronização concluída com sucesso!")
    } catch {
      case e: Exception =>
        println(s"\nErro durante a sincronização: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      fs.close()
    }
  }

  def syncDirectory(fs: FileSystem, source: Path, dest: Path, refDate: LocalDate): Unit = {
    val refMillis = refDate.atStartOfDay(ZoneId.systemDefault).toInstant.toEpochMilli

    fs.listStatus(source).filter(_.isDirectory).foreach { partition =>
      val partitionName = partition.getPath.getName
      val sourcePart = partition.getPath
      val destPart = new Path(dest, partitionName)

      if (partition.getModificationTime > refMillis) {
        println(s"  Copiando partição: $partitionName (modificada após ${refDate})")
        copyDirectory(fs, sourcePart, destPart)
      } else {
        println(s"  Ignorando partição: $partitionName (não modificada após ${refDate})")
      }
    }
  }

  def copyDirectory(fs: FileSystem, source: Path, dest: Path): Unit = {
    if (fs.exists(dest)) {
      fs.delete(dest, true) // Remove o diretório existente
    }

    fs.mkdirs(dest)

    fs.listStatus(source).foreach { status =>
      val itemPath = status.getPath
      val destPath = new Path(dest, itemPath.getName)

      if (status.isDirectory) {
        copyDirectory(fs, itemPath, destPath)
      } else {
        copyFile(fs, itemPath, destPath)
      }
    }
  }

  def copyFile(fs: FileSystem, source: Path, dest: Path): Unit = {
    val in = fs.open(source)
    val out = fs.create(dest)
    try {
      val buffer = new Array[Byte](4 * 1024 * 1024) // 4MB buffer
      var bytesRead = in.read(buffer)
      while (bytesRead > 0) {
        out.write(buffer, 0, bytesRead)
        bytesRead = in.read(buffer)
      }
    } finally {
      in.close()
      out.close()
    }
  }

  def verifyCount(spark: SparkSession, sourcePath: String, destPath: String): Unit = {
    try {
      val srcCount = spark.read.parquet(sourcePath).count()
      val dstCount = spark.read.parquet(destPath).count()

      println(s"  Verificação de contagem:")
      println(s"    Origem ($sourcePath): $srcCount registros")
      println(s"    Destino ($destPath): $dstCount registros")

      if (srcCount != dstCount) {
        println(s"    AVISO: Diferença encontrada! (${srcCount - dstCount} registros)")
      } else {
        println("    OK: Contagens idênticas")
      }
    } catch {
      case e: Exception =>
        println(s"  Erro ao verificar contagens: ${e.getMessage}")
    }
  }
}
// HDFSDirectorySync.main(Array())