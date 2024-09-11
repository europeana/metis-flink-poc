package eu.europeana.cloud.source;

/**
 * The exception indicating that something was wrong with coordinating and the state is somehow inconsistent.
 * It is some kind of assertion, we do not except it could happen during work even in case of infrastructure
 * failures. If it happened we should review the source code and implement the case properly.
 */
public class SourceConsistencyException extends RuntimeException{

  public SourceConsistencyException(String message) {
      super(message);
  }
}
