package org.flink.poc;

import java.io.Serializable;

public class TaskStatistics implements Serializable {

  String taskId;
  int failedRecordProcessed = 0;
  int succeededRecordProcessed = 0;

  public TaskStatistics() {
  }

  public TaskStatistics(String taskId) {
    this.taskId = taskId;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  @Override
  public String toString() {
    return "TaskStatistics{" +
        "taskId='" + taskId + '\'' +
        ", failedRecordProcessed=" + failedRecordProcessed +
        ", succeededRecordProcessed=" + succeededRecordProcessed +
        '}';
  }

  public int getFailedRecordProcessed() {
    return failedRecordProcessed;
  }

  public void setFailedRecordProcessed(int failedRecordProcessed) {
    this.failedRecordProcessed = failedRecordProcessed;
  }

  public int getSucceededRecordProcessed() {
    return succeededRecordProcessed;
  }

  public void setSucceededRecordProcessed(int succeededRecordProcessed) {
    this.succeededRecordProcessed = succeededRecordProcessed;
  }
}
