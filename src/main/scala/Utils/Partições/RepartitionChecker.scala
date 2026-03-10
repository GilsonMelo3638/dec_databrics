package Utils.Particições

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object RepartitionChecker {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BpeRepartitionChecker")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val basePath = "/datalake/bronze/sources/dbms/legado/dec/nfe_diario"
    val expectedFiles = 5

    def listDayPartitions(path: Path): Seq[Path] = {
      fs.listStatus(path)
        .filter(_.isDirectory)
        .flatMap { year =>
          fs.listStatus(year.getPath)
            .filter(_.isDirectory)
            .flatMap { month =>
              fs.listStatus(month.getPath)
                .filter(_.isDirectory)
                .map(_.getPath)
            }
        }
    }

    val dayPartitions = listDayPartitions(new Path(basePath))

    println(s"Iniciando verificação... (limite esperado: $expectedFiles arquivos)")

    dayPartitions.foreach { partition =>
      val parquetCount = fs.listStatus(partition)
        .count(_.getPath.getName.endsWith(".parquet"))

      if (parquetCount > expectedFiles) {
        println(s"$partition -> $parquetCount arquivos (ACIMA do limite)")
      }
    }

    println("Verificação concluída.")
  }
}

//RepartitionChecker.main(Array())