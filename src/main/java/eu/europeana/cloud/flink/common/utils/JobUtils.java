package eu.europeana.cloud.flink.common.utils;

import com.datastax.driver.core.utils.UUIDs;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

public class JobUtils {

  public static Properties readProperties(String propertyPath) throws IOException {
    Properties properties = new Properties();
    try (FileInputStream fileInput = new FileInputStream(propertyPath)) {
      properties.load(fileInput);
      return properties;
    }
  }

  @NotNull
  public static UUID useNewIfNull(String uuidString) {
    return Optional.ofNullable(uuidString)
                   .map(UUID::fromString)
                   .orElse(UUIDs.timeBased());
  }
}
