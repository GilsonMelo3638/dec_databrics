//spark-shell   --num-executors 40   --executor-memory 16G   --executor-cores 4   --driver-memory 8G   --conf "spark.driver.maxResultSize=4G"   --conf "spark.sql.parquet.writeLegacyFormat=true"   --conf "spark.sql.debug.maxToStringFields=100"   --conf "spark.executor.memoryOverhead=4096"   --conf "spark.network.timeout=1200s"   --conf "spark.executor.heartbeatInterval=300s"   --conf "spark.storage.replication=2"   --conf "spark.shuffle.service.enabled=true"   --conf "spark.shuffle.file.buffer=1m"   --conf "spark.reducer.maxSizeInFlight=96m"   --conf "spark.dynamicAllocation.enabled=true"   --conf "spark.dynamicAllocation.minExecutors=10"   --conf "spark.dynamicAllocation.maxExecutors=40"   --conf "spark.dynamicAllocation.initialExecutors=20"   --conf "spark.sql.shuffle.partitions=800"   --conf "spark.default.parallelism=800"   --conf "spark.memory.fraction=0.6"   --conf "spark.memory.storageFraction=0.5"   --conf "spark.sql.autoBroadcastJoinThreshold=-1"   --conf "spark.sql.hive.filesourcePartitionFileCacheSize=524288000"   --conf "spark.sql.shuffle.spill=true"   --packages "com.databricks:spark-xml_2.12:0.13.0"

package Utils.Particições


import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object VerificaChavesDuplicadas {
  def main(args: Array[String]): Unit = {
    // 1. Configuração robusta
    val spark = SparkSession.builder()
      .appName("Verificação de Duplicatas Estável")
      .config("spark.sql.shuffle.partitions", "2000")  // Melhor distribuição
      .config("spark.executor.memoryOverhead", "4G")   // 4GB de overhead
      .config("spark.memory.fraction", "0.6")          // Mais espaço para execução
      .config("spark.network.timeout", "1200s")        // Timeout aumentado
      .config("spark.sql.adaptive.enabled", "true")    // Otimização automática
      .getOrCreate()

    try {
      // 2. Parâmetros
      val basePath = "/datalake/bronze/sources/dbms/dec/nfce_diario/"
      val outputPath = "/datalake/bronze/sources/dbms/dec/nfce_duplicatas_final/"

      // 3. Limpeza inicial
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(outputPath))) {
        fs.delete(new Path(outputPath), true)
      }

      // 4. Processamento em etapas com checkpoint
      spark.sparkContext.setCheckpointDir("/temp/checkpoints")

      // 5. Leitura incremental com controle de memória
      val df = spark.read.option("basePath", basePath)
        .parquet(s"$basePath/year=*")
        .select("CHAVE", "year", "month", "day")
        .checkpoint()  // Quebra a linhagem para evitar OOM

      val totalChaves = df.count()
      val totalChavesDistintas = df.select("CHAVE").distinct().count()

      println("=" * 50)
      println(s"📊 Estatísticas de Chaves:")
      println(s"Total de chaves: $totalChaves")
      println(s"Total de chaves distintas: $totalChavesDistintas")
      println("=" * 50)

      // 6. Agregação em duas etapas
      val contagens = df.groupBy("CHAVE").count().filter("count > 1").cache()

      val resultado = contagens.join(df, "CHAVE")
        .groupBy("CHAVE", "count")
        .agg(collect_set(concat_ws("/", col("year"), col("month"), col("day"))).alias("datas"))
        .withColumn("periodos", array_join(col("datas"), " | "))
        .drop("datas")
        .orderBy(desc("count"))
        .limit(1000000)  // Limite seguro para evitar estouro

      // 7. Escrita otimizada
      resultado.write
        .mode("overwrite")
        .option("compression", "snappy")
        .option("maxRecordsPerFile", 100000)  // Arquivos menores
        .parquet(outputPath)

      // 8. Relatório
      println("=" * 50)
      println(s"📊 Estatísticas de Chaves:")
      println(s"Total de chaves: $totalChaves")
      println(s"Total de chaves distintas: $totalChavesDistintas")
      println(s"✅ Processamento concluído! ${resultado.count()} duplicatas encontradas")
      println("=" * 50)

    } catch {
      case e: Exception =>
        println(s"❌ Erro fatal: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

// Para executar:
//VerificaChavesDuplicadas.main(Array())