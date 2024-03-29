package eu.europeana.cloud.flink.common.sink;

import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import static java.lang.Integer.parseInt;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
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

  public static CassandraConnectionProvider getCassandraConnectionProvider(Properties properties) {
    return CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
        properties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS),
        Integer.parseInt(properties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT, "9042")),
        properties.getProperty(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
        properties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
        properties.getProperty(TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN));
  }
}

