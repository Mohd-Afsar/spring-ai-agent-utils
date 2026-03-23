package org.springaicommunity.nova.config;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the remote JanusGraph / Gremlin Server connection.
 *
 * <p>Activated only when {@code janusgraph.enabled=true} is set in
 * {@code application.properties}. This prevents the application from
 * failing at startup when JanusGraph is not available (e.g. local dev
 * without the graph DB port-forwarded).
 *
 * <h3>Connection topology</h3>
 * <pre>
 *   NOVA (local) ──SSH tunnel──► Gremlin Server :8182 ──► JanusGraph ──► Cassandra/HBase
 * </pre>
 *
 * <h3>Required graph schema</h3>
 * Vertex labels: Region, SubRegion, Site, Controller, Cell, Carrier<br>
 * Required vertex properties: {@code nodeId} (unique), {@code name},
 *   {@code technology}, {@code vendor}, {@code hierarchyLevel}, {@code alarmCount}<br>
 * Edge labels: {@code PARENT_OF} (hierarchy), {@code CONNECTED_TO} (transport links)<br>
 * Index: composite unique index on {@code nodeId} — mandatory for O(1) vertex lookup.
 *
 * @author Spring AI Community
 */
@Configuration
@ConditionalOnProperty(name = "janusgraph.enabled", havingValue = "true")
public class JanusGraphConfig {

	private static final Logger log = LoggerFactory.getLogger(JanusGraphConfig.class);

	@Value("${janusgraph.gremlin.host:localhost}")
	private String host;

	@Value("${janusgraph.gremlin.port:8182}")
	private int port;

	@Value("${janusgraph.gremlin.traversal-source:g}")
	private String traversalSource;

	@Value("${janusgraph.gremlin.connection-pool-size:4}")
	private int connectionPoolSize;

	@Value("${janusgraph.gremlin.connection-setup-timeout-ms:8000}")
	private int connectionSetupTimeoutMs;

	/**
	 * {@code graphson} (default) avoids {@code RelationIdentifier} decode errors with JanusGraph when using
	 * vanilla TinkerPop — GraphBinary requires JanusGraph-specific serializers on the client.
	 * Set to {@code graphbinary} only if you register JanusGraph IoRegistry on the serializer.
	 */
	@Value("${janusgraph.gremlin.serializer:graphson}")
	private String serializerKind;

	/**
	 * Gremlin {@link Cluster} manages the connection pool to the Gremlin Server.
	 * The {@code destroyMethod = "close"} ensures connections are released on
	 * Spring context shutdown.
	 */
	@Bean(destroyMethod = "close")
	public Cluster janusGraphCluster() {
		log.info("[JanusGraph] Connecting to Gremlin Server at {}:{} (traversal source: '{}', serializer: {})",
				host, port, traversalSource, serializerKind);
		var builder = Cluster.build()
				.addContactPoint(host)
				.port(port)
				.maxConnectionPoolSize(connectionPoolSize)
				.minConnectionPoolSize(1)
				.connectionSetupTimeoutMillis(connectionSetupTimeoutMs);
		if ("graphbinary".equalsIgnoreCase(serializerKind)) {
			builder.serializer(new GraphBinaryMessageSerializerV1());
		}
		else {
			// Default: GraphSON v3 — compatible with JanusGraph Gremlin Server without client-side JanusGraph jars.
			builder.serializer(new GraphSONMessageSerializerV3());
		}
		return builder.create();
	}

	/**
	 * Remote {@link GraphTraversalSource} bound to the Gremlin Server cluster.
	 * Inject this bean wherever Gremlin traversals are needed.
	 */
	@Bean
	public GraphTraversalSource graphTraversalSource(Cluster cluster) {
		log.info("[JanusGraph] Creating remote GraphTraversalSource (traversal source: '{}')", traversalSource);
		return AnonymousTraversalSource.traversal()
				.withRemote(DriverRemoteConnection.using(cluster, traversalSource));
	}

	/**
	 * Connectivity check at startup so operators see Gremlin/JanusGraph reachability
	 * even when the topology RCA tool is not invoked for a given request.
	 */
	@Bean
	@ConditionalOnProperty(name = "janusgraph.startup-healthcheck.enabled", havingValue = "true")
	public ApplicationRunner janusGraphStartupHealthCheck(GraphTraversalSource graphTraversalSource) {
		return (ApplicationArguments args) -> {
			long startNs = System.nanoTime();
			try {
				log.info("[JanusGraph] Startup health check Gremlin: graphTraversalSource.V().limit(1).count()");
				// Cheap; avoids Vertex/RelationIdentifier deserialization on GraphSON clients.
				Long sample = graphTraversalSource.V().limit(1).count().next();

				long elapsedMs = Math.max(1, (System.nanoTime() - startNs) / 1_000_000);
				log.info("[JanusGraph] Startup health check OK (limited vertex count: {}, elapsedMs: {})",
						sample, elapsedMs);
			}
			catch (Exception e) {
				long elapsedMs = Math.max(1, (System.nanoTime() - startNs) / 1_000_000);
				// Intentionally avoid logging the full stack trace here; if Gremlin/JanusGraph is down,
				// this can be noisy. We still keep the key error message for diagnosis.
				log.warn("[JanusGraph] Startup health check FAILED (elapsedMs: {}). Error: {}",
						elapsedMs, e.getMessage());
			}
		};
	}

}
