package eu.europeana.cloud.flink.xslt;

import eu.europeana.cloud.flink.common.FollowingTaskParams;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class XsltParams extends FollowingTaskParams {

 private String xsltUrl;
 private String metisDatasetId;
 private String metisDatasetName;
 private String metisDatasetCountry;
 private String metisDatasetLanguage;

}
