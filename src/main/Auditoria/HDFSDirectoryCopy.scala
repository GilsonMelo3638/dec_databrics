package Auditoria

package Utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object HDFSDirectoryCopy {
  def main(args: Array[String]): Unit = {
    // Caminhos de origem e destino
    val sourcePath = new Path("/datalake/prata/sources/dbms/dec/")
    val destinationPath = new Path("/datalake/prata/backup_producao/20250313/")

    // Configuração do Hadoop
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    // Tenta realizar a cópia
    val result = Try(copyDirectory(fs, sourcePath, destinationPath))

    // Trata o resultado da operação
    result match {
      case Success(_) =>
        println("Cópia concluída com sucesso!")
        // Inicializa o SparkSession
        val spark = SparkSession.builder()
          .appName("Verificação de Cópia")
          .getOrCreate()

        // Verifica a contagem de registros após a cópia
        verifyCopy(spark, sourcePath.toString, destinationPath.toString)

        // Encerra o SparkSession
        spark.stop()
      case Failure(exception) => println(s"Erro durante a cópia: ${exception.getMessage}")
    }
  }

  /**
   * Copia recursivamente um diretório do HDFS para outro local.
   *
   * @param fs          FileSystem do HDFS
   * @param source      Caminho de origem
   * @param destination Caminho de destino
   */
  def copyDirectory(fs: FileSystem, source: Path, destination: Path): Unit = {
    // Verifica se o diretório de origem existe
    if (!fs.exists(source)) {
      throw new RuntimeException(s"Diretório de origem não encontrado: ${source.toString}")
    }

    // Verifica se o diretório de destino existe; se não, cria
    if (!fs.exists(destination)) {
      println(s"Criando diretório de destino: ${destination.toString}")
      fs.mkdirs(destination)
    }

    // Lista todos os arquivos e subdiretórios no diretório de origem
    val files = fs.listStatus(source)
    for (file <- files) {
      val sourceFilePath = file.getPath
      val destinationFilePath = new Path(destination, sourceFilePath.getName)

      if (file.isDirectory) {
        // Se for um subdiretório, copia recursivamente
        println(s"Copiando subdiretório: ${sourceFilePath.toString} -> ${destinationFilePath.toString}")
        copyDirectory(fs, sourceFilePath, destinationFilePath)
      } else {
        // Se for um arquivo, copia para o destino
        println(s"Copiando arquivo: ${sourceFilePath.toString} -> ${destinationFilePath.toString}")
        copyFile(fs, sourceFilePath, destinationFilePath)
      }
    }
  }

  /**
   * Copia um arquivo no HDFS de um local para outro.
   *
   * @param fs          FileSystem do HDFS
   * @param source      Caminho de origem do arquivo
   * @param destination Caminho de destino do arquivo
   */
  def copyFile(fs: FileSystem, source: Path, destination: Path): Unit = {
    val in = fs.open(source) // Abre o arquivo de origem para leitura
    val out = fs.create(destination) // Cria o arquivo de destino para escrita
    try {
      val buffer = new Array[Byte](4 * 1024 * 1024) // Buffer de 4MB
      var bytesRead = in.read(buffer)
      while (bytesRead > 0) {
        out.write(buffer, 0, bytesRead) // Escreve os dados no arquivo de destino
        bytesRead = in.read(buffer)
      }
    } finally {
      in.close() // Fecha o stream de entrada
      out.close() // Fecha o stream de saída
    }
  }

  /**
   * Verifica a contagem de registros nos diretórios de origem e destino após a cópia.
   *
   * @param spark          SparkSession
   * @param sourcePath     Caminho de origem
   * @param destinationPath Caminho de destino
   */
  def verifyCopy(spark: SparkSession, sourcePath: String, destinationPath: String): Unit = {
    // Verifica a contagem de registros para os diretórios de NFe
    verifyCount(spark, s"$sourcePath/nfe/infNFe", s"$destinationPath/nfe/infNFe")
    verifyCount(spark, s"$sourcePath/nfe/det", s"$destinationPath/nfe/det")

    // Verifica a contagem de registros para os diretórios de NFCe
    verifyCount(spark, s"$sourcePath/nfce/infNFCe", s"$destinationPath/nfce/infNFCe")
    verifyCount(spark, s"$sourcePath/nfce/det", s"$destinationPath/nfce/det")
  }

  /**
   * Verifica a contagem de registros em um par de diretórios de origem e destino.
   *
   * @param spark          SparkSession
   * @param sourceDir      Caminho de origem
   * @param destinationDir Caminho de destino
   */
  def verifyCount(spark: SparkSession, sourceDir: String, destinationDir: String): Unit = {
    val sourceDF = spark.read.parquet(sourceDir)
    val destinationDF = spark.read.parquet(destinationDir)

    val sourceCount = sourceDF.count()
    val destinationCount = destinationDF.count()

    println(s"Contagem de registros em $sourceDir: $sourceCount")
    println(s"Contagem de registros em $destinationDir: $destinationCount")

    if (sourceCount == destinationCount) {
      println("As contagens de registros são iguais.")
    } else {
      println("AVISO: As contagens de registros são diferentes!")
    }
  }
}
//HDFSDirectoryCopy.main(Array())<