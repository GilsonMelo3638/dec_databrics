package MainProcessor

import Abstract.Cancelamento.{BPe, MDFe, NF3e, NFe, CTe, NFCe}
import Extrator.{diarioGenericoCancelamento}

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
  }
}
//SqoopCancelamentoProcessorApp.main(Array())