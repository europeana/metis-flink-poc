package org.flink.poc;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class FileManager {

  public String readFileData(String path) throws IOException {
    File file = new File(path);
    try (InputStream inputStream = new FileInputStream(file)) {
      return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  public void saveFileData(String path, String content) throws IOException {
    File file = new File(path);
    try (OutputStream outputStream = new FileOutputStream(file)) {
      outputStream.write(content.getBytes());
    }
  }
}
