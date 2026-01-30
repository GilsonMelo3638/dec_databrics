package Schemas

import org.apache.spark.sql.types._

object BPeEventoSchema {

  def createSchema(): StructType = {
    new StructType()
      .add("_dhConexao", StringType, nullable = true)
      .add("_ipTransmissor", StringType, nullable = true)
      .add("_nPortaCon", StringType, nullable = true)
      .add("_versao", StringType, nullable = true)
      .add("_xmlns", StringType, nullable = true)
      .add("eventoBPe",
        new StructType()
          .add("_versao", StringType, nullable = true)
          .add("infEvento",
            new StructType()
              .add("CNPJ", StringType, nullable = true)
              .add("_Id", StringType, nullable = true)
              .add("cOrgao", StringType, nullable = true)
              .add("chBPe", StringType, nullable = true)
              .add("detEvento",
                new StructType()
                  .add("_versaoEvento", StringType, nullable = true)
                  .add("evCancBPe",
                    new StructType()
                      .add("descEvento", StringType, nullable = true)
                      .add("nProt", StringType, nullable = true)
                      .add("xJust", StringType, nullable = true)
                  )
                  .add("evAlteracaoPoltrona",
                    new StructType()
                      .add("descEvento", StringType, nullable = true)
                      .add("nProt", StringType, nullable = true)
                      .add("poltrona", StringType, nullable = true)
                  )
                  .add("evExcessoBagagem",
                    new StructType()
                      .add("descEvento", StringType, nullable = true)
                      .add("nProt", StringType, nullable = true)
                      .add("qBagagem", DoubleType, nullable = true)
                      .add("vTotBag", DoubleType, nullable = true)
                  )

                  .add("evNaoEmbBPe",
                    new StructType()
                      .add("descEvento", StringType, nullable = true)
                      .add("nProt", StringType, nullable = true)
                      .add("xJust", StringType, nullable = true)
                  )
                  .add("evSubBPe",
                    new StructType()
                      .add("chBPeSubstituto", StringType, nullable = true)
                      .add("descEvento", StringType, nullable = true)
                      .add("dhRecbto", StringType, nullable = true)
                      .add("nProt", StringType, nullable = true)
                      .add("tpSub", StringType, nullable = true)
                  )
              )
              .add("dhEvento", StringType, nullable = true)
              .add("nSeqEvento", StringType, nullable = true)
              .add("tpAmb", StringType, nullable = true)
              .add("tpEvento", StringType, nullable = true)
          )
      )
      .add("retEventoBPe",
        new StructType()
          .add("_versao", StringType, nullable = true)
          .add("infEvento",
            new StructType()
              .add("_Id", StringType, nullable = true)
              .add("cOrgao", StringType, nullable = true)
              .add("cStat", StringType, nullable = true)
              .add("chBPe", StringType, nullable = true)
              .add("dhRegEvento", StringType, nullable = true)
              .add("nProt", StringType, nullable = true)
              .add("nSeqEvento", StringType, nullable = true)
              .add("tpAmb", StringType, nullable = true)
              .add("tpEvento", StringType, nullable = true)
              .add("verAplic", StringType, nullable = true)
              .add("xEvento", StringType, nullable = true)
              .add("xMotivo", StringType, nullable = true)
          )
      )
  }
}
