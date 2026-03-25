package org.springaicommunity.nova.config;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Configures the remote JanusGraph / Gremlin Server connection.
 *
 * <p>Activated only when {@code janusgraph.enabled=true} (set in
 * {@code classpath:janusgraph-client.properties}, imported from {@code application.properties}).
 * All Gremlin connection settings are defined there and bound via {@link JanusGraphClientProperties}.
 * This prevents the application from
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
@ConditionalOnProperty(name = "janusgraph.mode", havingValue = "remote", matchIfMissing = true)
@ConditionalOnProperty(name = "janusgraph.enabled", havingValue = "true")
public class JanusGraphConfig {

	private static final Logger log = LoggerFactory.getLogger(JanusGraphConfig.class);

	/**
	 * Single instance type for this demo (remote Gremlin client).
	 * Equivalent to your {@code JanusGraphInstance instance} argument.
	 */
	private static final String INSTANCE_DEFAULT = "DEFAULT";

	private static final String INSTANCE_NOT_NULL_MSG = "instance must not be null";

	/**
	 * Kept to mirror your code structure: {@code instance.toString() + TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING}.
	 * In this remote-client variant we don't need to open a local config file path;
	 * the properties are already bound by Spring from {@code janusgraph-client.properties}.
	 */
	private static final String TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING = ".clientPropsBoundBySpring";

	private final AtomicBoolean initialized = new AtomicBoolean(false);
	private final Map<String, Cluster> clustersByInstance = new ConcurrentHashMap<>();
	private final Map<String, GraphTraversalSource> traversalsByInstance = new ConcurrentHashMap<>();

	private final JanusGraphClientProperties clientProperties;

	public JanusGraphConfig(JanusGraphClientProperties clientProperties) {
		this.clientProperties = clientProperties;
	}

	/**
	 * Ensures {@code janusgraph.storage.*} from {@code janusgraph-client.properties} matches a
	 * coherent server profile and logs it next to the Gremlin client connection (NOVA does not
	 * open CQL; this mirrors the Janus host config for operators).
	 */
	private void validateAndLogServerStorageProfile() {
		var storage = clientProperties.getStorage();
		var cql = storage.getCql();
		String backend = storage.getBackend();
		if (StringUtils.hasText(backend) && "cql".equalsIgnoreCase(backend.trim())
				&& !StringUtils.hasText(cql.getKeyspace())) {
			throw new IllegalStateException(
					"janusgraph.storage.backend=cql requires janusgraph.storage.cql.keyspace (see janusgraph-client.properties).");
		}
		StringBuilder msg = new StringBuilder(128);
		msg.append("[JanusGraph] Documented server storage (janusgraph.storage.*):");
		if (StringUtils.hasText(backend)) {
			msg.append(" backend=").append(backend);
		}
		if (StringUtils.hasText(storage.getHostname())) {
			msg.append(" hostname=").append(storage.getHostname());
		}
		if (storage.getPort() > 0) {
			msg.append(" port=").append(storage.getPort());
		}
		if (StringUtils.hasText(cql.getKeyspace())) {
			msg.append(" cql.keyspace=").append(cql.getKeyspace());
		}
		if (StringUtils.hasText(cql.getReadConsistencyLevel())) {
			msg.append(" cql.readCL=").append(cql.getReadConsistencyLevel());
		}
		if (StringUtils.hasText(cql.getWriteConsistencyLevel())) {
			msg.append(" cql.writeCL=").append(cql.getWriteConsistencyLevel());
		}
		if (StringUtils.hasText(cql.getLocalDatacenter())) {
			msg.append(" cql.local-datacenter=").append(cql.getLocalDatacenter());
		}
		if (log.isInfoEnabled()) {
			log.info(msg.toString());
		}
	}

	/**
	 * Gremlin {@link Cluster} manages the connection pool to the Gremlin Server.
	 * The {@code destroyMethod = "close"} ensures connections are released on
	 * Spring context shutdown.
	 */
	@Bean(destroyMethod = "close")
	public Cluster janusGraphCluster() {
		ensureInitialized();
		return clustersByInstance.get(INSTANCE_DEFAULT);
	}

	/**
	 * Remote {@link GraphTraversalSource} bound to the Gremlin Server cluster.
	 * Inject this bean wherever Gremlin traversals are needed.
	 */
	@Bean
	public GraphTraversalSource graphTraversalSource() {
		ensureInitialized();
		return traversalsByInstance.get(INSTANCE_DEFAULT);
	}

	/**
	 * Mirrors your provided initialization structure:
	 * initGraphByInstance(instance) -> initGraph(properties, instance) -> initTraversal(instance).
	 */
	private void ensureInitialized() {
		if (initialized.get()) {
			return;
		}
		Map<String, String> result = initGraphByInstance(INSTANCE_DEFAULT);
		initialized.set(true);
		String status = result.get(INSTANCE_DEFAULT);
		log.info("[JanusGraph] initGraphByInstance result: {} => {}", INSTANCE_DEFAULT, status);
	}

	private Map<String, String> initGraphByInstance(String instance) {
		Objects.requireNonNull(instance, INSTANCE_NOT_NULL_MSG);
		Map<String, String> result = new HashMap<>();

		Map<String, String> propertiesMap = getJanusPropertiesByInstance(instance);
		try {
			if (validateProperties(propertiesMap)) {
				String properties = propertiesMap.get(instance + TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING);
				if (properties == null || properties.trim().isEmpty()) {
					throw new IllegalArgumentException("Graph properties cannot be null or empty for instance: " + instance);
				}

				initGraph(properties, instance);
				initTraversal(instance);

				result.put(instance, "GRAPH_INIT_SUCCESS");
			}
			else {
				result.put(instance, "GRAPH_INIT_INVALID_CONFIG_OR_PROPERTIES");
			}
		}
		catch (Exception e) {
			log.error("[JanusGraph] Exception in initGraphByInstance for instance={}: {}", instance, e.getMessage(), e);
			result.put(instance, "ERROR: " + e.getMessage());
		}
		return result;
	}

	private Map<String, String> getJanusPropertiesByInstance(String instance) {
		Objects.requireNonNull(instance, INSTANCE_NOT_NULL_MSG);
		Map<String, String> propertiesMap = new HashMap<>();
		// Remote-client variant: properties are already bound from janusgraph-client.properties via Spring.
		propertiesMap.put(instance + TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING, "bound janusgraph-client.properties");
		return propertiesMap;
	}

	private boolean validateProperties(Map<String, String> propertiesMap) {
		if (propertiesMap == null) return false;
		var gremlin = clientProperties.getGremlin();
		return StringUtils.hasText(gremlin.getHost())
				&& gremlin.getPort() > 0
				&& StringUtils.hasText(gremlin.getTraversalSource())
				&& StringUtils.hasText(gremlin.getSerializer());
	}

	private void initGraph(String properties, String instance) {
		Objects.requireNonNull(instance, INSTANCE_NOT_NULL_MSG);
		Objects.requireNonNull(properties, "properties must not be null");

		// If you ever extend this to multiple instances, close/re-init logic can live here.
		// For this demo we just initialize once (guarded by ensureInitialized()).
		var gremlin = clientProperties.getGremlin();
		String host = gremlin.getHost();
		int port = gremlin.getPort();
		String traversalSource = gremlin.getTraversalSource();
		String serializerKind = gremlin.getSerializer();
		int maxPool = gremlin.getConnectionPoolSize();
		int minPool = gremlin.getMinConnectionPoolSize();

		if (!StringUtils.hasText(host)) {
			throw new IllegalStateException(
					"janusgraph.gremlin.host is not set; configure it in janusgraph-client.properties (or env JANUSGRAPH_GREMLIN_HOST).");
		}
		if (port <= 0) {
			throw new IllegalStateException("janusgraph.gremlin.port must be positive (see janusgraph-client.properties).");
		}
		if (!StringUtils.hasText(traversalSource)) {
			throw new IllegalStateException("janusgraph.gremlin.traversal-source is not set (see janusgraph-client.properties).");
		}
		if (!StringUtils.hasText(serializerKind)) {
			throw new IllegalStateException("janusgraph.gremlin.serializer is not set (see janusgraph-client.properties).");
		}
		if (maxPool <= 0) {
			throw new IllegalStateException("janusgraph.gremlin.connection-pool-size must be positive.");
		}
		if (minPool <= 0 || minPool > maxPool) {
			throw new IllegalStateException(
					"janusgraph.gremlin.min-connection-pool-size must be >= 1 and <= janusgraph.gremlin.connection-pool-size.");
		}
		if (gremlin.getConnectionSetupTimeoutMs() <= 0) {
			throw new IllegalStateException("janusgraph.gremlin.connection-setup-timeout-ms must be positive.");
		}

		validateAndLogServerStorageProfile();
		log.info("[JanusGraph] Connecting to Gremlin Server at {}:{} (traversal source: '{}', serializer: {})",
				host, port, traversalSource, serializerKind);

		var builder = Cluster.build()
				.addContactPoint(host)
				.port(port)
				.maxConnectionPoolSize(maxPool)
				.minConnectionPoolSize(minPool)
				.connectionSetupTimeoutMillis(gremlin.getConnectionSetupTimeoutMs());

		if ("graphbinary".equalsIgnoreCase(serializerKind)) {
			builder.serializer(new GraphBinaryMessageSerializerV1());
		}
		else {
			// Default: GraphSON v3 — compatible with JanusGraph Gremlin Server without client-side JanusGraph jars.
			builder.serializer(new GraphSONMessageSerializerV3());
		}

		clustersByInstance.put(instance, builder.create());
	}

	private void initTraversal(String instance) {
		Objects.requireNonNull(instance, INSTANCE_NOT_NULL_MSG);
		Cluster cluster = clustersByInstance.get(instance);
		if (cluster == null) {
			throw new IllegalStateException("Cluster is not initialized for instance: " + instance);
		}

		String traversalSource = clientProperties.getGremlin().getTraversalSource();
		log.info("[JanusGraph] Creating remote GraphTraversalSource (traversal source: '{}')", traversalSource);
		GraphTraversalSource g = AnonymousTraversalSource.traversal()
				.withRemote(DriverRemoteConnection.using(cluster, traversalSource));

		traversalsByInstance.put(instance, g);
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

	/**
	 * Logs Gremlin reachability + global vertex/edge counts at startup.
	 * Can be expensive on very large graphs; enable only when needed.
	 */
	@Bean
	@ConditionalOnProperty(name = "janusgraph.startup-diagnostics.enabled", havingValue = "true")
	public ApplicationRunner janusGraphStartupDiagnostics(GraphTraversalSource graphTraversalSource) {
		return (ApplicationArguments args) -> {
			if (graphTraversalSource == null) {
				log.info("[JanusGraph] Startup diagnostics skipped (no GraphTraversalSource).");
				return;
			}

			var gremlin = clientProperties.getGremlin();
			String endpoint = (StringUtils.hasText(gremlin.getHost()) && gremlin.getPort() > 0)
					? gremlin.getHost() + ":" + gremlin.getPort()
					: "(see janusgraph.gremlin.host/port in janusgraph-client.properties)";

			String traversalSource = gremlin.getTraversalSource();
			String docKeyspace = clientProperties.getStorage() != null && clientProperties.getStorage().getCql() != null
					? clientProperties.getStorage().getCql().getKeyspace()
					: null;

			long t0 = System.nanoTime();
			try {
				long vertexCount = graphTraversalSource.V().count().next();
				long edgeCount = graphTraversalSource.E().count().next();
				long elapsedMs = Math.max(1L, (System.nanoTime() - t0) / 1_000_000L);

				log.info("Storage backend: " + clientProperties.getStorage().getBackend());
				log.info("Storage hostname: " + clientProperties.getStorage().getHostname());
				log.info("Storage port: " + clientProperties.getStorage().getPort());
				log.info("Storage cql keyspace: " + clientProperties.getStorage().getCql().getKeyspace());
				log.info("Storage cql read consistency level: " + clientProperties.getStorage().getCql().getReadConsistencyLevel());
				log.info("Storage cql write consistency level: " + clientProperties.getStorage().getCql().getWriteConsistencyLevel());
				log.info("Storage cql local datacenter: " + clientProperties.getStorage().getCql().getLocalDatacenter());
				log.info("[JanusGraph] Startup diagnostics: endpoint={}, traversalSource={}, docKeyspace={}, vertices={}, edges={}, elapsedMs={}",
						endpoint, traversalSource, docKeyspace != null ? docKeyspace : "(unset)", vertexCount, edgeCount, elapsedMs);
				System.out.println("[JanusGraph] connected=true | endpoint=" + endpoint
						+ " | traversal=" + traversalSource
						+ " | docKeyspace=" + (docKeyspace != null ? docKeyspace : "unset")
						+ " | vertexCount=" + vertexCount
						+ " | edgeCount=" + edgeCount
						+ " | elapsedMs=" + elapsedMs);
			}
			catch (Exception e) {
				long elapsedMs = Math.max(1L, (System.nanoTime() - t0) / 1_000_000L);
				log.warn("[JanusGraph] Startup diagnostics failed (endpoint={}, traversalSource={}): {} (elapsedMs={})",
						endpoint, traversalSource, e.getMessage(), elapsedMs);
			}
		};
	}

}
