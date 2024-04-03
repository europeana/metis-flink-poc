package eu.europeana.cloud.flink.validation;

import eu.europeana.cloud.flink.common.FollowingTaskParams;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ValidationTaskParams extends FollowingTaskParams {

 private String schemaName;

 private String rootLocation;

 private String schematronLocation;

}
