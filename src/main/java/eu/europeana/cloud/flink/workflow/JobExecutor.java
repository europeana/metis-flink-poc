package eu.europeana.cloud.flink.workflow;

import eu.europeana.cloud.flink.workflow.entities.JobDetails;
import eu.europeana.cloud.flink.workflow.entities.SubmitJobRequest;
import eu.europeana.cloud.flink.workflow.entities.SubmitJobResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

public class JobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);


  private final String serverUrl;
  private final String authHeader;
  private final String jarId;
  private final RestClient restClient;

  public JobExecutor(Properties serverConfiguration) {
    serverUrl = serverConfiguration.getProperty("job.manager.url");
    String user = serverConfiguration.getProperty("job.manager.user");
    String password = serverConfiguration.getProperty("job.manager.password");
    authHeader = "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    jarId = serverConfiguration.getProperty("jar.id");
    restClient = RestClient.builder()
                           .baseUrl(serverUrl)
                           .defaultHeader("Authorization", authHeader)
                           .build();
  }

  public void execute(SubmitJobRequest request) throws InterruptedException {
    String jobId = submitJob(request);
    System.out.print("Executing...");
    JobDetails details;
    do {
      Thread.sleep(500L);
      details = getProgress(jobId);
      System.out.print(".");
    } while (!details.getState().equals("FINISHED"));
    System.out.print("");
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
      LOGGER.info("Submitted Job: {} Result:\n{}", request, result);
      return result.getJobid();
    } catch (RestClientResponseException e) {
      String message = e.getMessage().replaceAll("\\\\n\\\\t", "\n");
      throw new RuntimeException("Error submitting job! " + message, e);
    }
  }

}
