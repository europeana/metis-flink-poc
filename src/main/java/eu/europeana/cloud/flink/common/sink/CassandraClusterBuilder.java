package eu.europeana.cloud.flink.common.sink;

import static java.lang.Integer.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import eu.europeana.cloud.copieddependencies.TopologyPropertyKeys;
import java.util.Properties;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class CassandraClusterBuilder extends ClusterBuilder {

  private final Properties properties;

  public CassandraClusterBuilder(Properties properties) {
    this.properties = properties;
  }

  @Override
  protected Cluster buildCluster(Builder builder) {
    return Cluster.builder()
                  .addContactPoints(properties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS).split(","))
                  .withPort(parseInt(properties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT, "9042")))
                  .withCredentials(
                      properties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
                      properties.getProperty(TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN))
                  .withProtocolVersion(ProtocolVersion.V3)
                  .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM)).build();
  }
}

