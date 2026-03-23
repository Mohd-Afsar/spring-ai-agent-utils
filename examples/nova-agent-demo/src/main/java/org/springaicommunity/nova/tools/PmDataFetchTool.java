package org.springaicommunity.nova.tools;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springaicommunity.nova.AgentConsole;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Combined PM analysis tool: fetches enriched PM data, runs the Java analytics
 * engine, and generates a professional NOC performance report — all in one step.
 *
 * <p>NOVA calls this single tool for any PM performance request. Internally it:
 * <ol>
 *   <li>Fetches enriched KPI time-series from the PM REST API (Cassandra + MySQL)</li>
 *   <li>Runs the {@link org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine}
 *       to pre-compute anomalies, scores, and findings in Java (no LLM token cost)</li>
 *   <li>Calls a specialist LLM analyst to turn the compact summary into a
 *       professional NOC performance report</li>
 * </ol>
 *
 * <p>Merging fetch + analyse into one tool eliminates the two-step tool-chaining
 * pattern that caused XML-format tool-call errors in the orchestrator model.
 */
public class PmDataFetchTool {

    private static final Logger log = LoggerFactory.getLogger(PmDataFetchTool.class);

    private static final String ANALYST_SYSTEM_PROMPT = """
            You are a senior Telecom NOC analyst.

            You receive a pre-computed PM analytics summary produced by a Java analytics engine \
            from Cassandra time-series data. The summary already contains:
            - Node identity and location
            - Health status and performance score (0-100)
            - Dimension scores (availability, throughput, reliability, resource efficiency)
            - Detected anomalies with KPI name, type (SPIKE/SUSTAINED_HIGH/GRADUAL_INCREASE/DIP), \
              severity (CRITICAL/HIGH/MEDIUM/LOW), deviation%, trend, and timestamp
            - Top busiest time periods
            - Pre-computed findings

            Your job is to turn this into a clean, professional NOC performance report.
            Write for a NOC manager who needs to act fast — be direct, precise, and jargon-appropriate.

            Rules:
            - Use only the values already in the summary — never invent numbers
            - Lead with the most critical issues
            - Explain what each anomaly means operationally, not just the numbers
            - End with 3-5 concrete, prioritised actions with clear owners and timelines
            - Keep the report concise — a NOC manager should read it in under 2 minutes
            """;

    private static final int MAX_RETRIES = 4;

    private final RestClient restClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine;
    private final ChatClient analystClient;

    private PmDataFetchTool(String pmApiBaseUrl,
            org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine,
            ChatClient.Builder builder) {
        this.restClient = RestClient.builder().baseUrl(pmApiBaseUrl).build();
        this.analyticsEngine = analyticsEngine;
        this.analystClient = builder.defaultSystem(ANALYST_SYSTEM_PROMPT).build();
    }

    // @formatter:off
    @Tool(name = "PmAnalysis", description = """
            Fetches PM (Performance Management) KPI data for a network node, runs anomaly
            detection and performance scoring, then generates a complete NOC performance report.

            Use this for ANY request involving: node performance, KPI analysis, traffic trends,
            utilisation, packet loss, ERAB/drop-rate analysis, or performance reports.

            This is a single tool — do NOT call PmDataFetch and PmAnalyst separately.
            Call this once with the node parameters and you will receive the full report.

            Parameters (all optional — blanks are filled with demo defaults so you can call this
            immediately for vague requests like "performance report"):
              domain       — default TRANSPORT
              vendor       — default NOKIA
              technology   — default NR
              dataLevel    — default NODE
              nodeName     — default "" (all nodes)
              granularity  — default HOURLY
              from / to    — default last 24 hours UTC if omitted
            """)
    public String fetchEnrichedPmData( // @formatter:on
            @ToolParam(required = false, description = "Network domain — blank → TRANSPORT") String domain,
            @ToolParam(required = false, description = "Vendor — blank → NOKIA") String vendor,
            @ToolParam(required = false, description = "Technology — blank → NR") String technology,
            @ToolParam(required = false, description = "Data level — blank → NODE") String dataLevel,
            @ToolParam(required = false, description = "Node IP/hostname — blank → all nodes") String nodeName,
            @ToolParam(required = false, description = "HOURLY, DAILY, WEEKLY — blank → HOURLY") String granularity,
            @ToolParam(required = false, description = "Start ISO-8601 UTC — blank → 24h ago") String from,
            @ToolParam(required = false, description = "End ISO-8601 UTC — blank → now") String to) {

        AgentConsole.toolStarted("PmAnalysis");
        try {
            ResolvedPmParams p = resolveParams(domain, vendor, technology, dataLevel, nodeName, granularity, from, to);
            domain = p.domain;
            vendor = p.vendor;
            technology = p.technology;
            dataLevel = p.dataLevel;
            nodeName = p.nodeName;
            granularity = p.granularity;
            from = p.from;
            to = p.to;

            boolean singleNode = nodeName != null && !nodeName.isBlank();

            // ── Step 1: Fetch enriched PM data ────────────────────────────────────
            // Single node → GET /pm/data/enriched (fast, direct partition lookup)
            // No node     → POST /pm/data/query/enriched (auto-discovers all nodes)
            List<org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse> responses;

            String raw;
            if (singleNode) {
                String url = UriComponentsBuilder.fromPath("/pm/data/enriched")
                        .queryParam("domain", domain)
                        .queryParam("vendor", vendor)
                        .queryParam("technology", technology)
                        .queryParam("dataLevel", dataLevel)
                        .queryParam("nodeName", nodeName)
                        .queryParam("granularity", granularity)
                        .queryParam("from", from)
                        .queryParam("to", to)
                        .build().toUriString();
                log.info("Fetching PM data (single node): {}", url);
                raw = restClient.get().uri(url).retrieve().body(String.class);
                responses = List.of(objectMapper.readValue(raw,
                        org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse.class));
            } else {
                String body = String.format(
                        "{\"domain\":\"%s\",\"vendor\":\"%s\",\"technology\":\"%s\","
                        + "\"dataLevel\":\"%s\",\"granularity\":\"%s\","
                        + "\"timeRange\":{\"from\":\"%s\",\"to\":\"%s\"}}",
                        domain, vendor, technology, dataLevel, granularity, from, to);
                log.info("Fetching PM data (all nodes) via POST /pm/data/query/enriched");
                raw = restClient.post()
                        .uri("/pm/data/query/enriched")
                        .header("Content-Type", "application/json")
                        .body(body)
                        .retrieve()
                        .body(String.class);
                responses = objectMapper.readValue(raw,
                        objectMapper.getTypeFactory().constructCollectionType(
                                List.class, org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse.class));
            }

            System.out.println();
            System.out.println("┌─ PM API RAW RESPONSE " + "─".repeat(46));
            System.out.println(raw);
            System.out.println("└" + "─".repeat(68));

            if (responses == null || responses.isEmpty()) {
                return "[PmAnalysis] No PM data found for the given parameters.";
            }

            // ── Step 2: Token-budget decision ─────────────────────────────────────
            // Estimate tokens using the standard GPT approximation (1 token ≈ 4 chars).
            // If the raw response fits within 100K tokens we send it verbatim so the LLM
            // can reason over the full time-series. Otherwise we compress it first via
            // the Java analytics engine to stay within context limits.
            int estimatedTokens = raw.length() / 4;
            String contextNode = singleNode ? nodeName : (responses.size() + " nodes");
            String reportContext = String.format(
                    "Performance analysis for %s (%s/%s/%s), %s granularity, %s to %s",
                    contextNode, domain, vendor, technology, granularity, from, to);

            final String prompt;
            if (estimatedTokens <= 100_000) {
                log.info("Raw response ~{} tokens — sending full data directly to LLM (no analytics compression)",
                        estimatedTokens);
                prompt = String.format("""
                        Investigation context: %s

                        Full enriched PM data (JSON):
                        %s

                        Generate the NOC performance report.
                        """, reportContext, raw);
            } else {
                // ── Step 2b: Run Java analytics engine on each node ───────────────
                log.info("Raw response ~{} tokens exceeds 100K limit — running analytics engine to compress",
                        estimatedTokens);
                List<org.springaicommunity.nova.pm.analytics.PmNodeSummary> summaries = new ArrayList<>();
                for (org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse enriched : responses) {
                    org.springaicommunity.nova.pm.analytics.PmNodeSummary s = analyticsEngine.analyze(enriched);
                    summaries.add(s);
                    log.info("Analytics complete — node={} health={} score={} anomalies={}",
                            s.getNode(), s.getHealth(), s.getPerformanceScore(),
                            s.getAnomalies() != null ? s.getAnomalies().size() : 0);
                }
                String summaryJson = objectMapper.writeValueAsString(
                        summaries.size() == 1 ? summaries.get(0) : summaries);
                log.info("Analytics summary: {} chars (~{} tokens)",
                        summaryJson.length(), summaryJson.length() / 4);
                prompt = String.format("""
                        Investigation context: %s

                        Pre-computed PM analytics summary (JSON):
                        %s

                        Generate the NOC performance report.
                        """, reportContext, summaryJson);
            }

            return callWithRetry(prompt);

        } catch (Exception e) {
            log.error("PM analysis failed for node={} from={} to={}", nodeName, from, to, e);
            return "[PM analysis error: " + e.getMessage()
                    + ". Verify the PM service is running and Cassandra is accessible.]";
        } finally {
            AgentConsole.toolFinished();
        }
    }

    private String callWithRetry(String prompt) {
        int waitSeconds = 2;
        System.out.println();
        System.out.println("┌─ PmAnalyst INPUT " + "─".repeat(50));
        System.out.println(prompt);
        System.out.println("└" + "─".repeat(68));

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                String result = this.analystClient.prompt(prompt).call().content();
                System.out.println();
                System.out.println("┌─ PmAnalyst OUTPUT " + "─".repeat(49));
                System.out.println(result);
                System.out.println("└" + "─".repeat(68));
                return result;
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : "";
                boolean isRateLimit = msg.contains("429") || msg.contains("rate_limit_exceeded");
                boolean isOverCapacity = msg.contains("503") || msg.contains("over capacity")
                        || msg.contains("TransientAiException");
                if ((!isRateLimit && !isOverCapacity) || attempt == MAX_RETRIES) {
                    throw e;
                }
                int wait = isOverCapacity ? Math.max(waitSeconds * 2, 30) : waitSeconds;
                log.warn("[PmAnalysis] {} — waiting {}s before retry {}/{}",
                        isOverCapacity ? "Over capacity" : "Rate limited", wait, attempt, MAX_RETRIES);
                try { Thread.sleep(wait * 1000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                waitSeconds = Math.min(waitSeconds * 2, 60);
            }
        }
        throw new IllegalStateException("Rate limit not resolved after " + MAX_RETRIES + " retries");
    }

    /**
     * Fills blanks so the orchestrator model can call PmAnalysis with no arguments
     * for generic "show performance report" requests.
     */
    private static ResolvedPmParams resolveParams(String domain, String vendor, String technology,
            String dataLevel, String nodeName, String granularity, String from, String to) {
        String d = blankToDefault(domain, "TRANSPORT");
        String v = blankToDefault(vendor, "NOKIA");
        String t = blankToDefault(technology, "NR");
        String dl = blankToDefault(dataLevel, "NODE");
        String nn = nodeName != null ? nodeName.trim() : "";
        String g = blankToDefault(granularity, "HOURLY").toUpperCase();
        Instant now = Instant.now();
        String fromIso = blankToDefault(from, DateTimeFormatter.ISO_INSTANT.format(now.minus(24, ChronoUnit.HOURS)));
        String toIso = blankToDefault(to, DateTimeFormatter.ISO_INSTANT.format(now));
        return new ResolvedPmParams(d, v, t, dl, nn, g, fromIso, toIso);
    }

    private static String blankToDefault(String s, String def) {
        return (s == null || s.isBlank()) ? def : s.trim();
    }

    private record ResolvedPmParams(String domain, String vendor, String technology, String dataLevel,
            String nodeName, String granularity, String from, String to) {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String pmApiBaseUrl = "http://localhost:8080";
        private org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine;
        private ChatClient.Builder chatClientBuilder;

        public Builder pmApiBaseUrl(String pmApiBaseUrl) {
            Assert.hasText(pmApiBaseUrl, "pmApiBaseUrl must not be blank");
            this.pmApiBaseUrl = pmApiBaseUrl;
            return this;
        }

        public Builder analyticsEngine(org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine engine) {
            this.analyticsEngine = engine;
            return this;
        }

        public Builder chatClientBuilder(ChatClient.Builder builder) {
            this.chatClientBuilder = builder;
            return this;
        }

        public PmDataFetchTool build() {
            Assert.notNull(analyticsEngine, "analyticsEngine must not be null");
            Assert.notNull(chatClientBuilder, "chatClientBuilder must not be null");
            return new PmDataFetchTool(pmApiBaseUrl, analyticsEngine, chatClientBuilder);
        }
    }
}
