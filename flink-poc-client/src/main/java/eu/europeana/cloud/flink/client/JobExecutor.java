package eu.europeana.cloud.flink.client;

import eu.europeana.cloud.flink.client.entities.JobDetails;
import eu.europeana.cloud.flink.client.entities.SubmitJobRequest;
import eu.europeana.cloud.flink.client.entities.SubmitJobResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

public class JobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);

  private final String authHeader;
  private final String jarId;
  private final RestClient restClient;

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
    authHeader = "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    restClient = RestClient.builder()
                           .baseUrl(serverUrl)
                           .defaultHeader("Authorization", authHeader)
                           .build();
    this.jarId = jarId;
  }

  public void execute(SubmitJobRequest request) throws InterruptedException {
    String jobId = submitJob(request);

    JobDetails details;
    int i=0;
    do {
      Thread.sleep(200L);
      details = getProgress(jobId);
      if(++i%5==0){
        LOGGER.info("Progress: {}", details);
      }
    } while (!details.getState().equals("FINISHED"));
    System.out.println("");
    LOGGER.info("Job finished! Details: {}", details);
  }

  public JobDetails getProgress(String jobId) {
    return restClient.get()
                     .uri("jobs/" + jobId)
                     .retrieve()
                     .body(JobDetails.class);
  }

  private String submitJob(SubmitJobRequest request) {
    try {
      SubmitJobResponse result =
          restClient.post()
                    .uri("jars/" + jarId + "/run?entry-class=" + request.getEntryClass())
                    .body(request)
                    .retrieve()
                    .body(SubmitJobResponse.class);
      LOGGER.info("Submitted Job: {} Submission result:\n{}\nExecuting...", request, result);
      return result.getJobid();
    } catch (RestClientResponseException e) {
      String message = e.getMessage().replaceAll("\\\\n\\\\t", "\n");
      throw new RuntimeException("Error submitting job! " + message, e);
    }
  }

}
