package MainProcessor

import DecCancelamentoProcessor.{BPeCancelamentoDiarioProcessor, NF3eCancelamentoDiarioProcessor, MDFeCancelamentoDiarioProcessor, NFCeCancelamentoDiarioProcessor}
import DecNFeEventoProcessor.NFeCancelamentoDiarioProcessor
import Sqoop.{SqoopGenericoCancelamentoProcessor, SqoopGenericoNFCeCancelamentoProcessor}

object SqoopCancelamentoProcessorApp {
  def main(args: Array[String]): Unit = {
    // Chama o método main da classe SqoopGenericoCancelamentoProcessor
    SqoopGenericoNFCeCancelamentoProcessor.main(Array())
    SqoopGenericoCancelamentoProcessor.main(args)
    BPeCancelamentoDiarioProcessor.main(Array())
    MDFeCancelamentoDiarioProcessor.main(Array())
    NF3eCancelamentoDiarioProcessor.main(Array())
    NFCeCancelamentoDiarioProcessor.main(Array())
    NFeCancelamentoDiarioProcessor.main(Array())
  }
}
//SqoopCancelamentoProcessorApp.main(Array())