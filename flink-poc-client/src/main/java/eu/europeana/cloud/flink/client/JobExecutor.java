package eu.europeana.cloud.flink.client;

import eu.europeana.cloud.flink.client.entities.JobDetails;
import eu.europeana.cloud.flink.client.entities.SubmitJobRequest;
import eu.europeana.cloud.flink.client.entities.SubmitJobResponse;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

public class JobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);
  private static final String STATE_FINISHED = "FINISHED";
  private static final String STATE_CANCELED = "CANCELED";
  private static final String STATE_FAILED = "FAILED";
  private static final Set<String> END_STATES = Set.of(STATE_FINISHED, STATE_FAILED, STATE_CANCELED);


  private final String jarId;
  private final RestTemplate restTemplate;
  private final String serverUrl;
  private final HttpHeaders httpHeader;

  public JobExecutor(Properties serverConfiguration) {
    this(serverConfiguration.getProperty("job.manager.url"),
        serverConfiguration.getProperty("job.manager.user"),
        serverConfiguration.getProperty("job.manager.password"),
        serverConfiguration.getProperty("jar.id"));
  }

  public JobExecutor(AbstractEnvironment serverConfiguration) {
    this(serverConfiguration.getProperty("job.manager.url"),
        serverConfiguration.getProperty("job.manager.user"),
        serverConfiguration.getProperty("job.manager.password"),
        serverConfiguration.getProperty("jar.id"));
  }

  public JobExecutor(String serverUrl, String user, String password, String jarId) {
    this.serverUrl = serverUrl;
    httpHeader = new HttpHeaders();
    httpHeader.setBasicAuth(user, password);
    restTemplate = new RestTemplate();
    this.jarId = jarId;
  }

  public void execute(SubmitJobRequest request) throws InterruptedException {
    String jobId = submitJob(request);

    JobDetails details;
    int i = 0;
    do {
      Thread.sleep(200L);
      details = getProgress(jobId);
      if (++i % 5 == 0) {
        LOGGER.info("Progress: {}", details);
      }
    } while (!END_STATES.contains(details.getState()));
    System.out.println("");
    if(!details.getState().equals(STATE_FINISHED)) {
      throw new RuntimeException("Job execution finished with state: " + details.getState());
    }

    LOGGER.info("Job finished! Details: {}", details);
  }

  public JobDetails getProgress(String jobId) {
    return restTemplate.exchange(serverUrl+"/jobs/" + jobId, HttpMethod.GET, new HttpEntity(httpHeader), JobDetails.class).getBody();
  }

  private String submitJob(SubmitJobRequest request) {
    SubmitJobResponse result = restTemplate.exchange(
        serverUrl + "/jars/" + jarId + "/run?entry-class=" + request.getEntryClass()
        , HttpMethod.POST, new HttpEntity<>(request, httpHeader), SubmitJobResponse.class).getBody();
    LOGGER.info("Submitted Job: {} Submission result:\n{}\nExecuting...", request, result);
    return result.getJobid();
  }

}
