package eu.europeana.cloud.flink.common;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class FileTuplePrintOperator implements MapFunction<FileTuple, FileTuple> {

  private final String info;

  public FileTuplePrintOperator() {
    this("");
  }

  public FileTuplePrintOperator(String info) {
    this.info = info;
  }

  @Override
  public FileTuple map(FileTuple file) throws Exception {
    System.out.print(info + " " + file.getResourceUrl() + " -> ");

    if (file.isMarkedAsDeleted()) {
      System.out.println(" DELETED ");
    } else {
      System.out.println(new String(file.getFileContent()));
    }

    return file;
  }
}
