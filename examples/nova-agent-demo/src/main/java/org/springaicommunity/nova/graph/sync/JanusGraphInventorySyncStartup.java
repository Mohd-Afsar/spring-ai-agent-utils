package org.springaicommunity.nova.graph.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Optionally hydrates JanusGraph from MySQL {@code railtel} on application startup so
 * {@link org.springaicommunity.nova.graph.GraphTopologyService} can resolve {@code nodeId} and
 * {@code PARENT_OF} / {@code CONNECTED_TO} traversals.
 */
@Configuration
@ConditionalOnProperty(name = "janusgraph.enabled", havingValue = "true")
@ConditionalOnBean(JanusGraphInventorySyncService.class)
public class JanusGraphInventorySyncStartup {

	private static final Logger log = LoggerFactory.getLogger(JanusGraphInventorySyncStartup.class);

	@Bean
	@Order(5)
	@ConditionalOnProperty(prefix = "janusgraph.inventory-sync", name = "enabled", havingValue = "true", matchIfMissing = true)
	@ConditionalOnProperty(prefix = "janusgraph.inventory-sync", name = "run-on-startup", havingValue = "true", matchIfMissing = true)
	public ApplicationRunner janusGraphInventorySyncRunner(JanusGraphInventorySyncService sync,
			@org.springframework.beans.factory.annotation.Value("${janusgraph.inventory-sync.drop-graph-before-sync:false}") boolean dropGraph,
			@org.springframework.beans.factory.annotation.Value("${janusgraph.inventory-sync.load-ospf-links:true}") boolean loadOspf,
			@org.springframework.beans.factory.annotation.Value("${janusgraph.inventory-sync.skip-if-nonempty:true}") boolean skipIfNonempty,
			@org.springframework.beans.factory.annotation.Value("${janusgraph.inventory-sync.force:false}") boolean force) {

		return args -> {
			try {
				log.info("[JanusSync] Startup inventory sync (dropGraph={}, loadOspf={}, skipIfNonempty={}, force={})",
						dropGraph, loadOspf, skipIfNonempty, force);
				sync.syncFromMysql(dropGraph, loadOspf, skipIfNonempty, force);
			}
			catch (Exception e) {
				log.warn("[JanusSync] Startup sync failed — RCA will see an empty graph until sync succeeds. Reason: {}",
						e.getMessage(), e);
			}
		};
	}
}
