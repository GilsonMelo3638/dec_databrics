package MainProcessor

import Abstract.Cancelamento.{BPe, CTe, MDFe, NF3e, NFCe, NFCom, NFe}
import Abstract.Evento.{CTeEvento, NFeEvento, BPeEvento, MDFeEvento, NF3eEvento, NFComEvento}
import Extrator.{diarioGenericoCancelamento, diarioGenericoEvento}
import RepartitionJob.RepartitionXlmCancelmentosProcessor
import org.apache.spark.sql.SparkSession

object SqoopCancelamentoProcessorApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SqoopCancelamentoProcessorApp")
      .enableHiveSupport()
      .getOrCreate()
    // Chama o método main da classe SqoopGenericoCancelamentoProcessor
    diarioGenericoCancelamento.main(args)
    BPe.process(spark)
    MDFe.process(spark)
    NF3e.process(spark)
    NFCom.process(spark)
    NFCe.process(spark)
    NFe.process(spark)
    CTe.process(spark)
    diarioGenericoEvento.main(args)
    CTeEvento.process(spark)
    NFeEvento.process(spark)
    BPeEvento.process(spark)
    MDFeEvento.process(spark)
    NF3eEvento.process(spark)
    NFComEvento.process(spark)

    // Executa o RepartitionXlmPequenosMediosProcessor como último processo
    try {
      println("=== Executando RepartitionXlmCancelmentosProcessor ===")

      // Chamada simples sem argumentos (configurações internas)
      RepartitionXlmCancelmentosProcessor.main(Array.empty)

      println("=== RepartitionXlmCancelmentosProcessor concluído com sucesso ===")
    } catch {
      case e: Exception =>
        println(s"Erro ao executar RepartitionXlmCancelmentosProcessor: ${e.getMessage}")
        e.printStackTrace()
    }

  }
}
//SqoopCancelamentoProcessorApp.main(Array())