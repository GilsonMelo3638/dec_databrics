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
package RepartitionJob

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class RepartitionXlmPequenosMediosProcessor(spark: SparkSession) {
  /**
   * Processa as partições de um caminho base, reparticionando os arquivos se necessário.
   *
   * @param basePath Caminho base da partição.
   * @param maxFiles Número máximo de arquivos permitidos por partição.
   * @param targetRepartition Número de partições para reparticionar.
   */
  def processPartitions(basePath: String, maxFiles: Int, targetRepartition: Int): Unit = {
    println(s"Processando partições em: $basePath")

    // Verifica se o caminho base existe
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path(basePath))) {
      System.err.println(s"O caminho base não existe: $basePath")
      return
    }

    // Lista todas as partições (subdiretórios) no caminho base
    val partitions = fs.listStatus(new Path(basePath)).filter(_.isDirectory).map(_.getPath.toString)

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
}

// Exemplo de uso
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RepartitionXlmPequenosMediosProcessor")
      .getOrCreate()

    // Instancia o reparticionador
    val repartitionProcessor = new RepartitionXlmPequenosMediosProcessor(spark)

    // Define os caminhos e configurações para cada tipo de documento
    val configs = Map(
      "CTe" -> ("/datalake/prata/sources/dbms/dec/cte/CTe", 10, 10),
      "CTeOS" -> ("/datalake/prata/sources/dbms/dec/cte/CTeOS", 2, 2),
      "GVTe" -> ("/datalake/prata/sources/dbms/dec/cte/GVTe", 2, 2),
      "BPe" -> ("/datalake/prata/sources/dbms/dec/bpe/BPe", 5, 5),
      "MDFe" -> ("/datalake/prata/sources/dbms/dec/mdfe/MDFe", 4, 4),
      "NF3e" -> ("/datalake/prata/sources/dbms/dec/nf3e/nf3e", 4, 4)
    )

    // Processa cada tipo de documento
    configs.foreach { case (docType, (basePath, maxFiles, targetRepartition)) =>
      println(s"Processando $docType...")
      repartitionProcessor.processPartitions(basePath, maxFiles, targetRepartition)
      println(s"Concluído processamento de $docType.")
    }
  }
}
//Main.main(Array())