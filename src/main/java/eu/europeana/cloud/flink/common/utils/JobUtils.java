package eu.europeana.cloud.flink.common.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class JobUtils {

  public static Properties readProperties(String propertyPath) throws IOException {
    Properties properties = new Properties();
    try (FileInputStream fileInput = new FileInputStream(propertyPath)) {
      properties.load(fileInput);
      return properties;
    }
  }

}
