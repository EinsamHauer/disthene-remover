package net.iponweb.disthene.remover;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

public class CassandraService {

    private final static Logger logger = LoggerFactory.getLogger(CassandraService.class);

    private static final int MAX_QUEUE_SIZE = 1024 * 1024;
    private static final int MAX_CONCURRENT_REQUESTS = 1024;
    private static final Pattern NORMALIZATION_PATTERN = Pattern.compile("[^0-9a-zA-Z_]");

    private final CqlSession session;
    private final PreparedStatement deleteStatement60;
    private final PreparedStatement deleteStatement900;
    private final PreparedStatement truncateStatement60;
    private final PreparedStatement truncateStatement900;


    public CassandraService(String contactPoint, String tenant) {
        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
                        .withStringList(DefaultDriverOption.CONTACT_POINTS, List.of(contactPoint + ":9042"))
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(1_000_000))
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ANY")
                        .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class)
                        .withClass(DefaultDriverOption.REQUEST_THROTTLER_CLASS, ConcurrencyLimitingRequestThrottler.class)
                        .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, MAX_CONCURRENT_REQUESTS)
                        .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, MAX_QUEUE_SIZE)
                        .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, CustomRetryPolicy.class)
                        .withInt(CustomDriverOption.CUSTOM_NUMBER_OF_RETRIES, 11)
                        .build();

        session = CqlSession.builder().withConfigLoader(loader).build();
        Metadata metadata = session.getMetadata();
        logger.info("Connected to cluster: " + metadata.getClusterName());
        for (Node node : metadata.getNodes().values()) {
            logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s",
                    node.getDatacenter(),
                    node.getBroadcastAddress().isPresent() ? node.getBroadcastAddress().get().toString() : "unknown", node.getRack()));
        }

        String table60 = String.format("metric.metric_%s_60", getNormalizedTenant(tenant));
        String table900 = String.format("metric.metric_%s_900", getNormalizedTenant(tenant));

        logger.info("Will use minute table: " + table60);
        logger.info("Will use 15-minute table: " + table900);

        deleteStatement60 = session.prepare("delete from " + table60 + " where path = ?");
        deleteStatement900 = session.prepare("delete from " + table900 + " where path = ?");
        truncateStatement60 = session.prepare("truncate table " + table60);
        truncateStatement900 = session.prepare("truncate table " + table900);
    }

    public void deleteMetric(String metric) {
        session.execute(deleteStatement60.bind(metric));
        session.execute(deleteStatement900.bind(metric));
    }

    public void truncateTenantTables() {
        session.execute(truncateStatement60.bind());
        session.execute(truncateStatement900.bind());
    }

    public void close() {
        logger.info("Cassandra session was shut down");
    }

    private static String getNormalizedTenant(String tenant) {
        return NORMALIZATION_PATTERN.matcher(tenant).replaceAll("_");
    }

}
