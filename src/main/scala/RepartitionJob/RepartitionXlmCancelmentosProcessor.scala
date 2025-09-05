package RepartitionJob

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object RepartitionXlmCancelmentosProcessor {
  val spark = SparkSession.builder().getOrCreate()
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)

  def main(args: Array[String]): Unit = {
    // Função genérica para processar partições
    def processPartitions(basePath: String, maxFiles: Int, targetRepartition: Int): Unit = {
      val partitions = fs.listStatus(new Path(basePath))
        .filter(_.isDirectory)
        .map(_.getPath.toString)

      partitions.foreach { partitionPath =>
        try {
          val fileCount = fs.listStatus(new Path(partitionPath)).count(_.getPath.getName.endsWith(".parquet"))
          println(s"Partição: $partitionPath, Arquivos: $fileCount")

          if (fileCount > maxFiles) {
            println(s"Reparticionando: $partitionPath")
            val df = spark.read.parquet(partitionPath)
            val tempPath = s"${partitionPath}_temp"

            // Reparticiona e salva os dados
            df.repartition(targetRepartition)
              .write
              .option("compression", "lz4")
              .mode("overwrite")
              .parquet(tempPath)

            // Remove a partição original e renomeia a temporária
            fs.delete(new Path(partitionPath), true)
            fs.rename(new Path(tempPath), new Path(partitionPath))

            println(s"Reparticionamento concluído para: $partitionPath")
          } else {
            println(s"Nenhuma ação necessária para: $partitionPath")
          }
        } catch {
          case e: Exception =>
            System.err.println(s"Erro ao processar a partição $partitionPath: ${e.getMessage}")
        }
      }
    }

    // Define os caminhos e configurações para cada tipo de documento
    val configs = Seq(
      ("/datalake/prata/sources/dbms/dec/nf3e/cancelamento", 2, 2),
      ("/datalake/prata/sources/dbms/dec/nfe/cancelamento", 2, 2),
      ("/datalake/prata/sources/dbms/dec/nfce/cancelamento", 2, 2),
      ("/datalake/prata/sources/dbms/dec/cte/cancelamento", 2, 2),
      ("/datalake/prata/sources/dbms/dec/bpe/cancelamento", 2, 2),
      ("/datalake/prata/sources/dbms/dec/nfcom/cancelamento", 2, 2),
      ("/datalake/prata/sources/dbms/dec/nfe/evento", 10, 10),
      ("/datalake/prata/sources/dbms/dec/mdfe/evento", 2, 2)
    )

    // Processar todas as configurações
    configs.foreach { case (path, maxFiles, targetRepartition) =>
      println(s"Processando caminho: $path")
      processPartitions(path, maxFiles, targetRepartition)
      println(s"Concluído processamento de: $path")
    }
  }
}

//RepartitionXlmCancelmentosProcessor.main(Array())