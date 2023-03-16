package org.flink.poc;

import eu.europeana.enrichment.rest.client.report.Report;
import java.util.Date;
import java.util.Set;

public class TaskData {

  private boolean isFailed = false;
  private String taskId;
  private String taskName;
  private String fileUrl;
  private Date taskStartDate;
  private String fileContent;
  private String resultFileContent;
  private String processingStatus;

  public Set<Report> getReportSet() {
    return reportSet;
  }

  public void setReportSet(Set<Report> reportSet) {
    this.reportSet = reportSet;
  }

  private Set<Report> reportSet;

  public TaskData() {
  }

  public TaskData(String taskId, String taskName, String fileUrl) {
    this(taskId, taskName, fileUrl, null, new Date(), null);
  }

  public TaskData(String taskId, String taskName, String fileUrl, String fileContent, Date taskStartDate,
      String resultFileContent) {
    this.taskId = taskId;
    this.taskName = taskName;
    this.fileUrl = fileUrl;
    this.taskStartDate = taskStartDate;
    this.fileContent = fileContent;
    this.resultFileContent = resultFileContent;
  }

  @Override
  public String toString() {
    return "TaskData{" +
        "isFailed=" + isFailed +
        ", taskId='" + taskId + '\'' +
        ", taskName='" + taskName + '\'' +
        ", fileUrl='" + fileUrl + '\'' +
        ", taskStartDate=" + taskStartDate +
        ", fileContent='" + fileContent + '\'' +
        ", resultFileContent='" + resultFileContent + '\'' +
        ", processingStatus='" + processingStatus + '\'' +
        '}';
  }

  public boolean isFailed() {
    return isFailed;
  }

  public void setFailed(boolean failed) {
    isFailed = failed;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public String getProcessingStatus() {
    return processingStatus;
  }

  public void setProcessingStatus(String processingStatus) {
    this.processingStatus = processingStatus;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public String getFileUrl() {
    return fileUrl;
  }

  public void setFileUrl(String fileUrl) {
    this.fileUrl = fileUrl;
  }

  public Date getTaskStartDate() {
    return taskStartDate;
  }

  public void setTaskStartDate(Date taskStartDate) {
    this.taskStartDate = taskStartDate;
  }

  public String getFileContent() {
    return fileContent;
  }

  public void setFileContent(String fileContent) {
    this.fileContent = fileContent;
  }

  public String getResultFileContent() {
    return resultFileContent;
  }

  public void setResultFileContent(String resultFileContent) {
    this.resultFileContent = resultFileContent;
  }
}
