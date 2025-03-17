package MainProcessor

import DecCancelamentoProcessor.{BPeCancelamentoDiarioProcessor, NF3eCancelamentoDiarioProcessor, NFCeCancelamentoDiarioProcessor}
import DecNFeEventoProcessor.NFeCancelamentoDiarioProcessor
import Sqoop.{SqoopGenericoCancelamentoProcessor, SqoopGenericoNFCeCancelamentoProcessor}

object SqoopCancelamentoProcessorApp {
  def main(args: Array[String]): Unit = {
    // Chama o método main da classe SqoopGenericoCancelamentoProcessor
    SqoopGenericoNFCeCancelamentoProcessor.main(Array())
    SqoopGenericoCancelamentoProcessor.main(args)
    BPeCancelamentoDiarioProcessor.main(Array())
    NF3eCancelamentoDiarioProcessor.main(Array())
    NFCeCancelamentoDiarioProcessor.main(Array())
    NFeCancelamentoDiarioProcessor.main(Array())
  }
}
//SqoopCancelamentoProcessorApp.main(Array())