package eu.europeana.cloud.flink.indexing;

import eu.europeana.cloud.flink.common.FollowingTaskParams;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class IndexingTaskParams extends FollowingTaskParams {

  private String metisDatasetId;
  private TargetIndexingDatabase database;
  private Date recordDate;
  private boolean preserveTimestamps;
  private boolean performRedirects;
  private List<String> datasetIdsForRedirection;
  private Properties indexingProperties;

}
