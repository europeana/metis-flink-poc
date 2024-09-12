package eu.europeana.cloud.retryable;

public class RetryableProxyCreateException extends RuntimeException {

  public RetryableProxyCreateException(ReflectiveOperationException e) {
    super("Could not create retry proxy!", e);
  }
}
