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
object RepartitionProcessor {
  val spark = SparkSession.builder().getOrCreate()
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)
  def main(args: Array[String]): Unit = {
  // Função genérica para processar partições
  def processPartitions(basePath: String, maxFiles: Int): Unit = {
    val partitions = fs.listStatus(new Path(basePath)).filter(_.isDirectory).map(_.getPath.toString)

    partitions.foreach { partitionPath =>
      val fileCount = fs.listStatus(new Path(partitionPath)).count(_.getPath.getName.endsWith(".parquet"))

      println(s"Partição: $partitionPath, Arquivos: $fileCount")

      if (fileCount > maxFiles) {
        println(s"Reparticionando: $partitionPath")
        val df = spark.read.parquet(partitionPath)
        val tempPath = s"${partitionPath}_temp"

        df.repartition(maxFiles).write.option("compression", "lz4").mode("overwrite").parquet(tempPath)

        fs.delete(new Path(partitionPath), true)
        fs.rename(new Path(tempPath), new Path(partitionPath))

        println(s"Reparticionamento concluído para: $partitionPath")
      } else {
        println(s"Nenhuma ação necessária para: $partitionPath")
      }
    }
  }

  // Definição dos diretórios e seus respectivos limites de reparticionamento
  val pathsWithLimits = Seq(
    ("/datalake/prata/sources/dbms/dec/nfe/infNFe", 40),
    ("/datalake/prata/sources/dbms/dec/nfe/det", 40),
    ("/datalake/prata/sources/dbms/dec/nfce/infNFCe", 60),
    ("/datalake/prata/sources/dbms/dec/nfce/det", 60)
  )

  // Processar todas as partições com seus respectivos limites
  pathsWithLimits.foreach { case (path, maxFiles) => processPartitions(path, maxFiles) }
}}

//RepartitionProcessor.main(Array())