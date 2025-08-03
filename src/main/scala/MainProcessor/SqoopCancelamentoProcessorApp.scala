package MainProcessor

import Abstract.Cancelamento.{BPe, CTe, MDFe, NF3e, NFCe, NFe}
import Abstract.Evento.NFeEvento
import Extrator.{diarioGenericoCancelamento, diarioGenericoEvento}
import RepartitionJob.RepartitionXlmCancelmentosProcessor

object SqoopCancelamentoProcessorApp {
  def main(args: Array[String]): Unit = {
    // Chama o método main da classe SqoopGenericoCancelamentoProcessor
    diarioGenericoCancelamento.main(args)
    BPe.main(Array())
    MDFe.main(Array())
    NF3e.main(Array())
    NFCe.main(Array())
    NFe.main(Array())
    CTe.main(Array())
    diarioGenericoEvento.main(args)
    NFeEvento.main(Array())
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