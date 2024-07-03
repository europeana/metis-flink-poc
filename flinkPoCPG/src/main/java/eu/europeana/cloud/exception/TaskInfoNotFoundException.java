package eu.europeana.cloud.exception;

public class TaskInfoNotFoundException extends Exception {

  public TaskInfoNotFoundException(long taskId) {
    super("Could not find task id: "+taskId);
  }
}
