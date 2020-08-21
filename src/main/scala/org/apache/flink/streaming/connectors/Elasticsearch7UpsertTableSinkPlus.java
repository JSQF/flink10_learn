package org.apache.flink.streaming.connectors;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch7.Elasticsearch7UpsertTableSink;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import sun.misc.BASE64Encoder;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.*;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-21
 * @Time 16:38
 */
public class Elasticsearch7UpsertTableSinkPlus extends Elasticsearch7UpsertTableSink {
    private String userName;

    private String passWord;
    final static BASE64Encoder encoder = new BASE64Encoder();

    public Elasticsearch7UpsertTableSinkPlus(boolean isAppendOnly,
                                             TableSchema schema,
                                             List<Host> hosts,
                                             String index,
                                             String keyDelimiter,
                                             String keyNullLiteral,
                                             SerializationSchema<Row> serializationSchema,
                                             XContentType contentType,
                                             ActionRequestFailureHandler failureHandler,
                                             Map<SinkOption, String> sinkOptions, String userName, String passWord) {
        super(isAppendOnly, schema, hosts, index, keyDelimiter, keyNullLiteral, serializationSchema, contentType, failureHandler, sinkOptions);
        this.userName = userName;
        this.passWord = passWord;
    }

    @Override
    protected SinkFunction<Tuple2<Boolean, Row>> createSinkFunction(
            List<Host> hosts,
            ActionRequestFailureHandler failureHandler,
            Map<SinkOption, String> sinkOptions,
            ElasticsearchUpsertSinkFunction upsertSinkFunction) {

        final List<HttpHost> httpHosts = hosts.stream()
                .map((host) -> new HttpHost(host.hostname, host.port, host.protocol))
                .collect(Collectors.toList());

        final ElasticsearchSink.Builder<Tuple2<Boolean, Row>> builder = createBuilder(upsertSinkFunction, httpHosts);

        builder.setFailureHandler(failureHandler);

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_MAX_ACTIONS))
                .ifPresent(v -> builder.setBulkFlushMaxActions(Integer.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_MAX_SIZE))
                .ifPresent(v -> builder.setBulkFlushMaxSizeMb(MemorySize.parse(v).getMebiBytes()));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_INTERVAL))
                .ifPresent(v -> builder.setBulkFlushInterval(Long.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_ENABLED))
                .ifPresent(v -> builder.setBulkFlushBackoff(Boolean.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_TYPE))
                .ifPresent(v -> builder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_RETRIES))
                .ifPresent(v -> builder.setBulkFlushBackoffRetries(Integer.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_DELAY))
                .ifPresent(v -> builder.setBulkFlushBackoffDelay(Long.valueOf(v)));

        DefaultRestClientFactory restClient = new DefaultRestClientFactory(sinkOptions.get(REST_PATH_PREFIX));
        if(!"".equalsIgnoreCase(userName) && !"".equalsIgnoreCase(passWord)){
            restClient.setUserName(userName);
            restClient.setPassWord(passWord);
        }
        builder.setRestClientFactory(restClient);

        final ElasticsearchSink<Tuple2<Boolean, Row>> sink = builder.build();

        Optional.ofNullable(sinkOptions.get(DISABLE_FLUSH_ON_CHECKPOINT))
                .ifPresent(v -> {
                    if (Boolean.valueOf(v)) {
                        sink.disableFlushOnCheckpoint();
                    }
                });

        return sink;
    }

    ElasticsearchSink.Builder<Tuple2<Boolean, Row>> createBuilder(
            ElasticsearchUpsertSinkFunction upsertSinkFunction,
            List<HttpHost> httpHosts) {
        return new ElasticsearchSink.Builder<>(httpHosts, upsertSinkFunction);
    }

    static class DefaultRestClientFactory implements RestClientFactory {

        private String pathPrefix;

        private String userName;

        private String passWord;

        public DefaultRestClientFactory(@Nullable String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (pathPrefix != null) {
                restClientBuilder.setPathPrefix(pathPrefix);
            }
            if(!"".equalsIgnoreCase(userName) && !"".equalsIgnoreCase(passWord)){
                Header[] header = new Header[1];
                BasicHeader authHeader = new BasicHeader("Authorization", "Basic " + encoder.encode((userName + ":" + passWord).getBytes()));
                header[0] = authHeader;
                restClientBuilder.setDefaultHeaders(header);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Elasticsearch7UpsertTableSinkPlus.DefaultRestClientFactory that = (Elasticsearch7UpsertTableSinkPlus.DefaultRestClientFactory) o;
            return Objects.equals(pathPrefix, that.pathPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathPrefix);
        }

        public String getPathPrefix() {
            return pathPrefix;
        }

        public void setPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassWord() {
            return passWord;
        }

        public void setPassWord(String passWord) {
            this.passWord = passWord;
        }
    }


}
