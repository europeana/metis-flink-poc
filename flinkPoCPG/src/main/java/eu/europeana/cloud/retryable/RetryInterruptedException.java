package eu.europeana.cloud.retryable;

public class RetryInterruptedException extends RuntimeException {

  public RetryInterruptedException(Throwable e) {
    super("Stopped waiting for retry, because the thread was interrupted!", e);
  }
}
