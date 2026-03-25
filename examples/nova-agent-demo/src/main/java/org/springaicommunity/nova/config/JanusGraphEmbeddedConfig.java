package org.springaicommunity.nova.config;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Embedded JanusGraph mode: opens JanusGraph directly (same as gremlin.sh):
 * {@code graph = JanusGraphFactory.open('conf/janusgraph-cql.properties')}.
 *
 * <p>Use this when your CLI embedded traversal shows vertices/edges but the Gremlin Server
 * endpoint seen by the app is empty or points to a different keyspace.
 */
@Configuration
@ConditionalOnProperty(name = "janusgraph.mode", havingValue = "embedded")
@ConditionalOnProperty(name = "janusgraph.enabled", havingValue = "true", matchIfMissing = true)
public class JanusGraphEmbeddedConfig {

	private static final Logger log = LoggerFactory.getLogger(JanusGraphEmbeddedConfig.class);

	@Value("${janusgraph.embedded.config-path:conf/janusgraph-cql.properties}")
	private String configPath;

	@Bean(destroyMethod = "close")
	public JanusGraph janusGraph() {
		if (!StringUtils.hasText(configPath)) {
			throw new IllegalStateException("janusgraph.embedded.config-path must be set for embedded mode.");
		}
		log.info("[JanusGraph][embedded] Opening JanusGraph config at path='{}'", configPath);
		return JanusGraphFactory.open(configPath.trim());
	}

	@Bean
	public GraphTraversalSource graphTraversalSource(JanusGraph graph) {
		log.info("[JanusGraph][embedded] Creating traversal source from embedded graph");
		return graph.traversal();
	}

	@Bean
	@ConditionalOnProperty(name = "janusgraph.startup-diagnostics.enabled", havingValue = "true", matchIfMissing = true)
	public ApplicationRunner janusGraphEmbeddedStartupDiagnostics(GraphTraversalSource g) {
		return (ApplicationArguments args) -> {
			long t0 = System.nanoTime();
			try {
				long v = g.V().count().next();
				long e = g.E().count().next();
				long elapsedMs = Math.max(1L, (System.nanoTime() - t0) / 1_000_000L);
				log.info("[JanusGraph][embedded] Startup diagnostics: vertices={}, edges={}, elapsedMs={}", v, e, elapsedMs);
				System.out.println("[JanusGraph][embedded] connected=true | vertexCount=" + v + " | edgeCount=" + e + " | elapsedMs=" + elapsedMs);
			}
			catch (Exception ex) {
				long elapsedMs = Math.max(1L, (System.nanoTime() - t0) / 1_000_000L);
				log.warn("[JanusGraph][embedded] Startup diagnostics failed: {} (elapsedMs={})", ex.getMessage(), elapsedMs);
				System.out.println("[JanusGraph][embedded] connected=false | error=" + ex.getMessage());
			}
		};
	}
}

