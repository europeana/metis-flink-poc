package eu.europeana.cloud.flink.common;

import com.datastax.driver.core.utils.UUIDs;
import java.io.Serializable;
import java.util.UUID;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@ToString
@EqualsAndHashCode
public class TaskInformation implements Serializable {

   @Builder.Default
   private UUID executionId= UUIDs.timeBased();

}
