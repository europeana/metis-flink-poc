package eu.europeana.cloud.flink.common.sink;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import eu.europeana.cloud.flink.common.tuples.NotificationTuple;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

/**
 * Creates CassandraSink that saves data based on flink Tuple* type (Tuple4)
 *
 */
public class TupleSinkBuilder {

  private final Properties properties;

  public TupleSinkBuilder(Properties properties) {
    this.properties=properties;
  }

  public void build(DataStream<NotificationTuple> stream) throws Exception {
    SingleOutputStreamOperator<Tuple4<Long, String, String, String>>
        finalStream = stream.map(
        /**
         * It needs to create class (could be anonymous) instead of lambda. In other case we got
         * the exception with message:
         * The generic type parameters of 'Tuple4' are missing. In many cases lambda methods don't
         * provide enough information for automatic type extraction when Java generics are involved.
         * An easy workaround is to use an (anonymous) class instead that implements the
         * 'org.apache.flink.api.common.functions.MapFunction' interface. Otherwise the type has to
         * be specified explicitly using type information.
         */
        new MapFunction<>() {
          @Override
          public Tuple4<Long, String, String, String> map(NotificationTuple tuple) throws Exception {
            return tuple.toGenericTuple();
          }
        }
    );
    CassandraSink.addSink(finalStream)
                 .setClusterBuilder(new CassandraClusterBuilder(properties))
                 .setQuery("INSERT INTO flink_poc.results(task_id,resource,result_resource,info_text) values(?,?,?,?)")
                 .setMapperOptions(() -> new Option[]{Mapper.Option.saveNullFields(true)})
                 .build();
  }
}
