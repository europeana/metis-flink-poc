package eu.europeana.cloud.flink.oai.source;

import eu.europeana.cloud.flink.oai.OAITaskParams;
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

public class OAIHeadersSource implements Source<OaiRecordHeader, OAISplit, OAIEnumeratorState>, ResultTypeQueryable<OaiRecordHeader> {

  private final OAITaskParams taskParams;

  public OAIHeadersSource(OAITaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public Boundedness getBoundedness() {
    //TODO Check if it is proper value
    return Boundedness.BOUNDED;
  }

  @Override
  public SplitEnumerator<OAISplit, OAIEnumeratorState> createEnumerator(
      SplitEnumeratorContext<OAISplit> enumContext) throws Exception {
    return new OAIHeadersSplitEnumerator(enumContext, null);
  }

  @Override
  public SourceReader<OaiRecordHeader, OAISplit> createReader(SourceReaderContext readerContext) throws Exception {
    return new OAIHeadersReader(readerContext, taskParams);
  }

  @Override
  public SplitEnumerator<OAISplit, OAIEnumeratorState> restoreEnumerator(SplitEnumeratorContext<OAISplit> enumContext, OAIEnumeratorState checkpoint)
      throws Exception {
    return new OAIHeadersSplitEnumerator(enumContext, checkpoint);
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
  public SimpleVersionedSerializer<OAIEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new OAIEnumeratorStateSerializer();
  }


  @Override
  public TypeInformation<OaiRecordHeader> getProducedType() {
    return TypeInformation.of(OaiRecordHeader.class);
  }

}
