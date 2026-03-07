package Utils.CorrecaoDados

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

object RemocaoDuplicidadesDet {

  case class ResultadoDeduplicacao(
                                    particao: String,
                                    totalAntes: Long,
                                    totalDepois: Long,
                                    removidos: Long,
                                    duracaoSegundos: Long,
                                    sucesso: Boolean,
                                    mensagem: String
                                  )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Remover Duplicidades - Produção")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    def pathExiste(path: String): Boolean =
      fs.exists(new Path(path))

    def moverSaidaTemporaria(tmpPath: String, finalPath: String): Unit = {
      val tmp = new Path(tmpPath)
      val finalP = new Path(finalPath)

      if (fs.exists(finalP)) {
        fs.delete(finalP, true)
      }

      fs.rename(tmp, finalP)
    }

    // ============================
    // Parâmetros (podem vir via args)
    // ============================

    val basePath = "/datalake/prata/sources/dbms/dec"
    val tipoArquivo = "nfe"
    val tipoInformacao = "det"

    val particoes =
      if (args.nonEmpty) args.toSeq
      else Seq("2006")

    val colunasChave = Seq("chave", "nitem")
    val sufixoSaida = "_dedup"
    val repartitionSaida = 40
    val compressao = "lz4"
    val modoWrite = "overwrite"

    // ============================
    // Processamento
    // ============================

    val resultados = particoes.map { chaveParticao =>

      val inicio = System.currentTimeMillis()

      val inputPath =
        s"$basePath/$tipoArquivo/$tipoInformacao/chave_particao=$chaveParticao"

      val outputPath =
        s"$basePath/$tipoArquivo/$tipoInformacao/chave_particao=${chaveParticao}$sufixoSaida"

      val tmpOutputPath = outputPath + "_tmp"

      Try {

        require(pathExiste(inputPath), s"Path não existe: $inputPath")

        println(s"Iniciando partição: $chaveParticao")

        val df = spark.read.parquet(inputPath)
          .persist(StorageLevel.MEMORY_AND_DISK)

        val totalAntes = df.count()

        val dfDeduplicado = df.dropDuplicates(colunasChave)

        val totalDepois = dfDeduplicado.count()

        println(s"Total Antes: $totalAntes")
        println(s"Total Depois: $totalDepois")
        println(s"Removidos: ${totalAntes - totalDepois}")

        dfDeduplicado
          .repartition(repartitionSaida)
          .write
          .format("parquet")
          .option("compression", compressao)
          .mode(modoWrite)
          .save(tmpOutputPath)

        moverSaidaTemporaria(tmpOutputPath, outputPath)

        df.unpersist()

        val fim = System.currentTimeMillis()

        ResultadoDeduplicacao(
          particao = chaveParticao,
          totalAntes = totalAntes,
          totalDepois = totalDepois,
          removidos = totalAntes - totalDepois,
          duracaoSegundos = (fim - inicio) / 1000,
          sucesso = true,
          mensagem = "Processado com sucesso"
        )

      } match {

        case Success(resultado) =>
          println(s"[SUCESSO] ${resultado.particao}")
          resultado

        case Failure(ex) =>
          val fim = System.currentTimeMillis()
          println(s"[ERRO] Partição $chaveParticao → ${ex.getMessage}")

          ResultadoDeduplicacao(
            particao = chaveParticao,
            totalAntes = 0,
            totalDepois = 0,
            removidos = 0,
            duracaoSegundos = (fim - inicio) / 1000,
            sucesso = false,
            mensagem = ex.getMessage
          )
      }
    }

    println("===== RESUMO FINAL =====")
    resultados.foreach(println)
  }
}

//RemocaoDuplicidades.main(Array())
//RemocaoDuplicidades.main(Array("2006","2007","2008"))