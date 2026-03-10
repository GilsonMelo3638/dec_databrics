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
package Utils.Particições

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.util.Calendar

object manualRepartition {
  val spark = SparkSession.builder().getOrCreate()
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)

  def main(args: Array[String]): Unit = {
    // Função genérica para processar partições
    def processPartitions(basePath: String, maxFiles: Int, diaSemana: Boolean, isNfceDet: Boolean = false): Unit = {
      val partitions = fs.listStatus(new Path(basePath))
        .filter(_.isDirectory)
        .map(_.getPath.toString)

      // Extrai o valor numérico da partição e ordena
      val sortedPartitions = partitions
        .map { path =>
          val partitionValue = path.split("=")(1).toInt // Extrai o valor numérico
          (partitionValue, path) // Retorna uma tupla (valor, caminho)
        }
        .sortBy(_._1) // Ordena pelo valor numérico
        .map(_._2) // Mantém apenas o caminho

      // Se for o diretório especial (/datalake/prata/sources/dbms/dec/nfce/det), aplica a regra especial
      if (isNfceDet) {
        if (diaSemana) {
          // Se for domingo, reparticiona todas as partições
          sortedPartitions.foreach { partitionPath =>
            repartitionIfNeeded(partitionPath, maxFiles)
          }
        } else {
          // Se não for domingo, reparticiona apenas a última e a penúltima partição
          val lastPartition = sortedPartitions.last
          val secondLastPartition = sortedPartitions(sortedPartitions.length - 2)

          repartitionIfNeeded(lastPartition, maxFiles)
          repartitionIfNeeded(secondLastPartition, maxFiles)
        }
      } else {
        // Para outros diretórios, reparticiona todas as partições em todos os dias
        sortedPartitions.foreach { partitionPath =>
          repartitionIfNeeded(partitionPath, maxFiles)
        }
      }
    }

    // Função para repartitionar se necessário
    def repartitionIfNeeded(partitionPath: String, maxFiles: Int): Unit = {
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

    // Verifica se hoje é domingo
    val calendar = Calendar.getInstance()
    val diaSemana = calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY

    // Definição dos diretórios e seus respectivos limites de reparticionamento
    val pathsWithLimits = Seq(
      ("/datalake/prata/sources/dbms/dec/nfe/infNFe", 40, false),
      ("/datalake/prata/sources/dbms/dec/nfe/det", 40, false),
      ("/datalake/prata/sources/dbms/dec/nfce/infNFCe", 60, false),
      ("/datalake/prata/sources/dbms/dec/nfce/det", 60, true) // Diretório especial
    )

    // Processar todas as partições com seus respectivos limites
    pathsWithLimits.foreach { case (path, maxFiles, isNfceDet) =>
      processPartitions(path, maxFiles, diaSemana, isNfceDet)
    }
  }
}
//manualRepartition.main(Array())