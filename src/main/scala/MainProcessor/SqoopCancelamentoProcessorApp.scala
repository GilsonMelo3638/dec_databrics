package MainProcessor

import Abstract.Cancelamento.{BPeCancelamentoDiarioProcessor, MDFeCancelamentoDiarioProcessor, NF3eCancelamentoDiarioProcessor, NFCeCancelamentoDiarioProcessor}
import DecDiarioProcessor.Cancelamento.NFe
import Extrator.{diarioNFCeCancelamento, diarioGenericoCancelamento}

object SqoopCancelamentoProcessorApp {
  def main(args: Array[String]): Unit = {
    // Chama o método main da classe SqoopGenericoCancelamentoProcessor
    diarioNFCeCancelamento.main(Array())
    diarioGenericoCancelamento.main(args)
    BPeCancelamentoDiarioProcessor.main(Array())
    MDFeCancelamentoDiarioProcessor.main(Array())
    NF3eCancelamentoDiarioProcessor.main(Array())
    NFCeCancelamentoDiarioProcessor.main(Array())
    NFe.main(Array())
  }
}
//SqoopCancelamentoProcessorApp.main(Array())