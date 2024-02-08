package eu.europeana.cloud.flink.oai.source;

import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class OAISource implements Source<OaiRecordHeader, OAISplit, Void>, ResultTypeQueryable<OaiRecordHeader> {
  @Override
  public Boundedness getBoundedness() {
    //TODO Check if it is proper value
    return Boundedness.BOUNDED;
  }

  @Override
  public SplitEnumerator<OAISplit, Void> createEnumerator(
      SplitEnumeratorContext<OAISplit> enumContext) throws Exception {
    return new OAISplitEnumerator(enumContext);
  }

  @Override
  public SourceReader<OaiRecordHeader, OAISplit> createReader(SourceReaderContext readerContext) throws Exception {
    return new OAIReader(readerContext);
  }

  @Override
  public SplitEnumerator<OAISplit, Void> restoreEnumerator(SplitEnumeratorContext<OAISplit> enumContext, Void checkpoint)
      throws Exception {
    return new OAISplitEnumerator(enumContext);
  }

  @Override
  public SimpleVersionedSerializer<OAISplit> getSplitSerializer() {

    return new SimpleVersionedSerializer<>() {
      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public byte[] serialize(OAISplit obj) throws IOException {
        return new byte[0];
      }

      @Override
      public OAISplit deserialize(int version, byte[] serialized) throws IOException {
        return new OAISplit();
      }
    };
  }

  @Override
  public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
    return null;
  }


  @Override
  public TypeInformation<OaiRecordHeader> getProducedType() {
    return TypeInformation.of(OaiRecordHeader.class);
  }

}
