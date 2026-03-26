package org.springaicommunity.nova.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

import javax.sql.DataSource;

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
    private static final int MAX_LLM_INPUT_TOKENS_ESTIMATE = 18_000;
    private static final int DEFAULT_TOP_N_NODES_FOR_LLM = 80;
    private static final int DEFAULT_TOP_N_ANOMALIES_FOR_LLM = 200;

    private static final String ANALYST_SYSTEM_PROMPT = loadSystemPrompt();

    private static String loadSystemPrompt() {
        try (InputStream in = PmDataFetchTool.class.getResourceAsStream("/prompt/PM_ANALYST_SYSTEM_PROMPT.md")) {
            if (in == null) {
                return "You are a senior Telecom NOC analyst. Produce a structured Performance Analysis Report using only provided values.";
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            LoggerFactory.getLogger(PmDataFetchTool.class).warn("[PmAnalysis] Could not load system prompt: {}", e.getMessage());
            return "You are a senior Telecom NOC analyst. Produce a structured Performance Analysis Report using only provided values.";
        }
    }

    private static final int MAX_RETRIES = 4;
    private static final String DEFAULT_DOMAIN = "TRANSPORT";
    private static final String DEFAULT_VENDOR = "JUNIPER";
    private static final String DEFAULT_TECHNOLOGY = "COMMON";
    private static final String DEFAULT_DATA_LEVEL = "ROUTER_COMMON_Router";
    private static final String DEFAULT_GRANULARITY = "HOURLY";
    private static final String DEFAULT_FROM = "2025-01-23T00:00:00Z";
    private static final String DEFAULT_TO = "2025-01-23T10:59:59Z";
    private static final String DATA_LEVEL_L0 = "L0_COMMON_Router";
    private static final String DATA_LEVEL_L1 = "L1_COMMON_Router";
    private static final String DATA_LEVEL_L2 = "L2_COMMON_Router";
    private static final String DATA_LEVEL_L3 = "L3_COMMON_Router";
    private static final Pattern MARKDOWN_BOLD = Pattern.compile("\\*\\*");
    private static final Pattern MARKDOWN_CODE = Pattern.compile("`");
    private static final Pattern BULLET_PREFIX = Pattern.compile("(?m)^\\s*[-*]\\s+");
    private static final Pattern HEADING_PREFIX = Pattern.compile("(?m)^\\s*#{1,6}\\s*");
    private static final Pattern SENTENCE_SPLIT = Pattern.compile("(?<=[.!?])\\s+");

    private final RestClient restClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine;
    private final ChatClient analystClient;
    private final DataSource dataSource;

    private PmDataFetchTool(String pmApiBaseUrl,
            org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine,
            ChatClient.Builder builder,
            DataSource dataSource) {
        this.restClient = RestClient.builder().baseUrl(pmApiBaseUrl).build();
        this.analyticsEngine = analyticsEngine;
        this.analystClient = builder.defaultSystem(ANALYST_SYSTEM_PROMPT).build();
        this.dataSource = dataSource;
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
              vendor       — default JUNIPER
              technology   — default COMMON
              dataLevel    — default ROUTER_COMMON_Router
              nodeName     — default "" (all nodes)
              granularity  — default HOURLY
              from         — default 2025-01-23T00:00:00Z
              to           — default 2025-01-23T10:59:59Z
            """)
    public String fetchEnrichedPmData( // @formatter:on
            @ToolParam(required = false, description = "Network domain — blank → TRANSPORT") String domain,
            @ToolParam(required = false, description = "Vendor — blank → JUNIPER") String vendor,
            @ToolParam(required = false, description = "Technology — blank → COMMON") String technology,
            @ToolParam(required = false, description = "Data level — blank → ROUTER_COMMON_Router") String dataLevel,
            @ToolParam(required = false, description = "Node IP/hostname — blank → all nodes") String nodeName,
            @ToolParam(required = false, description = "HOURLY, DAILY, WEEKLY — blank → HOURLY") String granularity,
            @ToolParam(required = false, description = "Start ISO-8601 UTC — blank → 2025-01-23T00:00:00Z") String from,
            @ToolParam(required = false, description = "End ISO-8601 UTC — blank → 2025-01-23T10:59:59Z") String to,
            @ToolParam(required = false, description = "Original user PM ask. Include this to control tone: only if user asked 'report' return formal report, else return conversational NOC guidance.") String userQuery) {

        AgentConsole.toolStarted("PmAnalysis");
        try {
            ResolvedPmParams p = resolveParams(domain, vendor, technology, dataLevel, nodeName, granularity, from, to,
                    userQuery == null ? "" : userQuery.trim());
            domain = p.domain;
            vendor = p.vendor;
            technology = p.technology;
            dataLevel = p.dataLevel;
            nodeName = p.nodeName;
            granularity = p.granularity;
            from = p.from;
            to = p.to;
            String originalUserQuery = userQuery == null ? "" : userQuery.trim();
            boolean reportRequested = isReportRequested(originalUserQuery);
            List<String> requestedKpiTerms = extractRequestedKpiTerms(originalUserQuery);
            boolean unfilteredAllCombos = isBlank(domain) && isBlank(vendor) && isBlank(technology)
                    && (nodeName == null || nodeName.isBlank());
            Set<String> requestedKpiCodes = unfilteredAllCombos
                    ? Set.of()
                    : resolveKpiCodesForTerms(requestedKpiTerms, domain, vendor, technology);

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
                        .queryParam("kpiCodes", requestedKpiCodes.toArray())
                        .build().toUriString();
                log.info("Fetching PM data (single node): {}", url);
                raw = restClient.get().uri(url).retrieve().body(String.class);
                responses = List.of(objectMapper.readValue(raw,
                        org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse.class));
            } else {
                if (unfilteredAllCombos) {
                    List<DomainVendorTech> combos = fetchSupportedDomainVendorTechCombos(5000);
                    if (combos.isEmpty()) {
                        return "[PmAnalysis] No (domain/vendor/technology) combinations found in KPI metadata. "
                                + "Cannot run an unfiltered PM query.";
                    }
                    log.info("Fetching PM data (unfiltered): {} domain/vendor/technology combos via POST /pm/data/query/enriched",
                            combos.size());
                    responses = new ArrayList<>();
                    raw = "";
                    for (DomainVendorTech c : combos) {
                        String body = buildEnrichedQueryBody(
                                c.domain(), c.vendor(), c.technology(),
                                dataLevel, granularity, requestedKpiCodes, from, to);
                        String comboRaw = restClient.post()
                                .uri("/pm/data/query/enriched")
                                .header("Content-Type", "application/json")
                                .body(body)
                                .retrieve()
                                .body(String.class);
                        List<org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse> comboResponses =
                                objectMapper.readValue(comboRaw,
                                        objectMapper.getTypeFactory().constructCollectionType(
                                                List.class, org.springaicommunity.nova.pm.dto.PmDataEnrichedResponse.class));
                        if (comboResponses != null && !comboResponses.isEmpty()) {
                            responses.addAll(comboResponses);
                        }
                    }
                } else {
                    String body = buildEnrichedQueryBody(domain, vendor, technology, dataLevel, granularity,
                            requestedKpiCodes, from, to);
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
            }

            System.out.println();
            System.out.println("┌─ PM API RAW RESPONSE " + "─".repeat(46));
            if (unfilteredAllCombos) {
                System.out.println("[unfiltered all-combos query] totalNodes=" + (responses == null ? 0 : responses.size()));
            } else {
                System.out.println(raw);
            }
            System.out.println("└" + "─".repeat(68));

            if (responses == null || responses.isEmpty()) {
                return "[PmAnalysis] No PM data found for the given parameters.";
            }

            if (!unfilteredAllCombos && !requestedKpiTerms.isEmpty() && requestedKpiCodes.isEmpty()) {
                return "token_usage_total_estimated=0\nI couldn’t find KPI metadata matching your requested KPI scope. Please share the exact KPI name/code.";
            }

            raw = objectMapper.writeValueAsString(singleNode ? responses.get(0) : responses);
            log.info("========pm_data_input start=========\n{}\n========pm_data_input end=========", raw);

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
            String responseMode = reportRequested ? "REPORT" : "CONVERSATIONAL";

            final String prompt;
            // if (estimatedTokens <= 100_000) {
            //     log.info("Raw response ~{} tokens — sending full data directly to LLM (no analytics compression)",
            //             estimatedTokens);
            //     prompt = String.format("""
            //             Investigation context: %s
            //             User PM request: %s
            //             Response mode: %s

            //             Full enriched PM data (JSON):
            //             %s

            //             If response mode is REPORT, generate a formal structured NOC report.
            //             If response mode is CONVERSATIONAL, respond like a friendly senior NOC engineer:
            //             - short actionable explanation
            //             - call out important KPI risks in plain operational language
            //             - suggest practical next checks/steps
            //             - avoid rigid report sections/headings unless explicitly asked
            //             """, reportContext, originalUserQuery.isBlank() ? "(not provided)" : originalUserQuery, responseMode, raw);
            // } else {
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
                Object summaryPayload = (summaries.size() == 1 ? summaries.get(0) : summaries);
                String summaryJson = objectMapper.writeValueAsString(summaryPayload);
                int summaryTokens = summaryJson.length() / 4;
                log.info("Analytics summary: {} chars (~{} tokens) nodes={}",
                        summaryJson.length(), summaryTokens, summaries.size());

                if (summaryTokens > MAX_LLM_INPUT_TOKENS_ESTIMATE) {
                    Object reduced = reduceForLlm(summaries, DEFAULT_TOP_N_NODES_FOR_LLM, DEFAULT_TOP_N_ANOMALIES_FOR_LLM);
                    summaryJson = objectMapper.writeValueAsString(reduced);
                    log.info("Reduced analytics summary for LLM: {} chars (~{} tokens)",
                            summaryJson.length(), summaryJson.length() / 4);
                }
                prompt = String.format("""
                        Investigation context: %s
                        User PM request: %s
                        Response mode: %s

                        Pre-computed PM analytics summary (JSON):
                        %s

                        If response mode is REPORT, generate a formal structured NOC report.
                        If response mode is CONVERSATIONAL, respond like a friendly senior NOC engineer:
                        - provide enough detail to act without follow-up questions
                        - include a short "What I see" plus "Next checks" bullet list (5-8 items)
                        - call out KPI risks/trends with the numbers that are present
                        - avoid rigid report sections/headings unless explicitly asked
                        """, reportContext, originalUserQuery.isBlank() ? "(not provided)" : originalUserQuery, responseMode, summaryJson);
            // }

            String llmOutput = callWithRetry(prompt);
            // Return the LLM output verbatim (no truncation, no reformatting, no prefixes).
            return llmOutput;

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
        log.info("========input start=========\n{}\n========input end=========", prompt);

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
                int hintedWait = extractRetryAfterSeconds(msg);
                int wait = hintedWait > 0 ? hintedWait : (isOverCapacity ? Math.max(waitSeconds * 2, 30) : waitSeconds);
                log.warn("[PmAnalysis] {} — waiting {}s before retry {}/{}",
                        isOverCapacity ? "Over capacity" : "Rate limited", wait, attempt, MAX_RETRIES);
                try { Thread.sleep(wait * 1000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                waitSeconds = Math.min(waitSeconds * 2, 60);
            }
        }
        throw new IllegalStateException("Rate limit not resolved after " + MAX_RETRIES + " retries");
    }

    private static int extractRetryAfterSeconds(String msg) {
        if (msg == null || msg.isBlank()) return -1;
        // Matches: "Please try again in 4.4112s"
        var m = Pattern.compile("try again in\\s+([0-9]+(?:\\.[0-9]+)?)s", Pattern.CASE_INSENSITIVE).matcher(msg);
        if (!m.find()) return -1;
        try {
            double seconds = Double.parseDouble(m.group(1));
            return (int) Math.ceil(seconds) + 1; // add 1s buffer
        } catch (Exception ignore) {
            return -1;
        }
    }

    private static Object reduceForLlm(List<org.springaicommunity.nova.pm.analytics.PmNodeSummary> summaries,
            int topNodes, int topAnomalies) {
        if (summaries == null || summaries.isEmpty()) return List.of();

        // Health counts
        Map<String, Long> healthCounts = summaries.stream()
                .collect(Collectors.groupingBy(s -> String.valueOf(s.getHealth()), Collectors.counting()));

        // Worst nodes by score (then by anomaly count)
        List<org.springaicommunity.nova.pm.analytics.PmNodeSummary> worstNodes = summaries.stream()
                .sorted(Comparator
                        .comparingInt(org.springaicommunity.nova.pm.analytics.PmNodeSummary::getPerformanceScore)
                        .thenComparingInt(s -> s.getAnomalies() == null ? 0 : -s.getAnomalies().size()))
                .limit(Math.max(1, topNodes))
                .toList();

        // Flatten anomalies and keep most severe / highest deviation
        record FlatA(String node, String domain, String vendor, String technology,
                     org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly a) {}

        List<Map<String, Object>> topA = summaries.stream()
                .flatMap(s -> (s.getAnomalies() == null ? List.<org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly>of() : s.getAnomalies())
                        .stream()
                        .map(a -> new FlatA(s.getNode(), s.getDomain(), s.getVendor(), s.getTechnology(), a)))
                .sorted(Comparator
                        .comparing((FlatA x) -> severityRank(x.a().getSeverity())).reversed()
                        .thenComparingDouble((FlatA x) -> x.a().getDeviationPct()).reversed())
                .limit(Math.max(1, topAnomalies))
                .map(x -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("node", x.node());
                    m.put("domain", x.domain());
                    m.put("vendor", x.vendor());
                    m.put("technology", x.technology());
                    m.put("kpiCode", x.a().getKpiCode());
                    m.put("kpiName", x.a().getKpiName());
                    m.put("severity", x.a().getSeverity());
                    m.put("type", x.a().getType());
                    m.put("deviationPct", x.a().getDeviationPct());
                    m.put("mean", x.a().getMean());
                    m.put("peak", x.a().getPeak());
                    m.put("detectedAt", x.a().getDetectedAt());
                    m.put("trend", x.a().getTrend());
                    return m;
                })
                .toList();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodesTotal", summaries.size());
        out.put("healthCounts", healthCounts);
        out.put("worstNodesTop", worstNodes);
        out.put("topAnomalies", topA);
        out.put("topThresholdBreaches", summaries.stream()
                .flatMap(s -> {
                    List<org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiThresholdBreach> breaches =
                            (s.getThresholdBreaches() == null)
                                    ? List.of()
                                    : s.getThresholdBreaches();
                    return breaches.stream().map(b -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("node", s.getNode());
                        m.put("domain", s.getDomain());
                        m.put("vendor", s.getVendor());
                        m.put("technology", s.getTechnology());
                        m.put("kpiCode", b.getKpiCode());
                        m.put("kpiName", b.getKpiName());
                        m.put("severity", b.getSeverity());
                        m.put("thresholdType", b.getThresholdType());
                        m.put("actualValue", b.getActualValue());
                        m.put("thresholdValue", b.getThresholdValue());
                        m.put("deviationPct", b.getDeviationPct());
                        m.put("detectedAt", b.getDetectedAt());
                        m.put("trend", b.getTrend());
                        return m;
                    });
                })
                .sorted(Comparator
                        .comparing((Map<String, Object> m) -> String.valueOf(m.get("severity"))).reversed()
                        .thenComparingDouble(m -> {
                            Object v = m.get("deviationPct");
                            return v instanceof Number n ? n.doubleValue() : 0.0;
                        }).reversed())
                .limit(200)
                .toList());
        out.put("note", "Reduced summary to fit LLM token budget; raw data was analyzed in Java across all nodes.");
        return out;
    }

    private static int severityRank(org.springaicommunity.nova.pm.analytics.PmNodeSummary.KpiAnomaly.Severity s) {
        if (s == null) return 0;
        return switch (s) {
            case LOW -> 1;
            case MEDIUM -> 2;
            case HIGH -> 3;
            case CRITICAL -> 4;
        };
    }

    private static String buildEnrichedQueryBody(String domain, String vendor, String technology,
            String dataLevel, String granularity, Set<String> requestedKpiCodes, String from, String to) {
        return String.format(
                "{\"domain\":\"%s\",\"vendor\":\"%s\",\"technology\":\"%s\","
                + "\"dataLevel\":\"%s\",\"granularity\":\"%s\",\"kpiCodes\":%s,"
                + "\"timeRange\":{\"from\":\"%s\",\"to\":\"%s\"}}",
                jsonEscape(domain), jsonEscape(vendor), jsonEscape(technology), jsonEscape(dataLevel),
                jsonEscape(granularity), toJsonArray(requestedKpiCodes), jsonEscape(from), jsonEscape(to));
    }

    private List<DomainVendorTech> fetchSupportedDomainVendorTechCombos(int maxRows) {
        if (this.dataSource == null) return List.of();
        int limit = Math.max(1, maxRows);
        String sql = "SELECT DISTINCT DOMAIN, VENDOR, TECHNOLOGY FROM KPI_FORMULA "
                + "WHERE (DELETED = 0 OR DELETED IS NULL) "
                + "AND DOMAIN IS NOT NULL AND TRIM(DOMAIN) <> '' "
                + "AND VENDOR IS NOT NULL AND TRIM(VENDOR) <> '' "
                + "AND TECHNOLOGY IS NOT NULL AND TRIM(TECHNOLOGY) <> '' "
                + "LIMIT " + limit;
        List<DomainVendorTech> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                java.sql.Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String d = rs.getString(1);
                String v = rs.getString(2);
                String t = rs.getString(3);
                if (!isBlank(d) && !isBlank(v) && !isBlank(t)) {
                    out.add(new DomainVendorTech(d.trim(), v.trim(), t.trim()));
                }
            }
        }
        catch (SQLException e) {
            log.warn("[PmAnalysis] Failed to discover domain/vendor/technology combos from KPI_FORMULA: {}", e.getMessage());
            return List.of();
        }
        return List.copyOf(out);
    }

    private record DomainVendorTech(String domain, String vendor, String technology) {}

    private static String jsonEscape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Fills blanks so the orchestrator model can call PmAnalysis with no arguments
     * for generic "show performance report" requests.
     */
    private ResolvedPmParams resolveParams(String domain, String vendor, String technology,
            String dataLevel, String nodeName, String granularity, String from, String to, String userQuery) {
        String nn = nodeName != null ? nodeName.trim() : "";
        String dl = blankToDefault(dataLevel, DEFAULT_DATA_LEVEL);
        String g = blankToDefault(granularity, DEFAULT_GRANULARITY).toUpperCase();
        String fromIso = blankToDefault(from, DEFAULT_FROM);
        String toIso = blankToDefault(to, DEFAULT_TO);

        String vendorFromQuery = extractVendorFromUserQuery(userQuery);
        ScopeResolution scope = resolveScopeFromUserQuery(userQuery);
        if (isBlank(dataLevel) && scope.dataLevel != null) {
            dl = scope.dataLevel;
        }
        if (isBlank(nodeName) && scope.nodeName != null && !scope.nodeName.isBlank()) {
            nn = scope.nodeName;
        }

        // Decide whether the user truly requested an "unfiltered" query (all domain/vendor/technology combos).
        // If scope resolution produced a nodeName (e.g. CITY=Hyderabad), this is not an unfiltered query and
        // we should apply the normal defaults for domain/vendor/technology.
        boolean unfilteredAllCombos = isBlank(domain) && isBlank(vendor) && isBlank(technology) && nn.isBlank();

        String d = unfilteredAllCombos ? "" : blankToDefault(domain, DEFAULT_DOMAIN);
        String v = unfilteredAllCombos ? "" : blankToDefault(vendor, DEFAULT_VENDOR);
        String t = unfilteredAllCombos ? "" : blankToDefault(technology, DEFAULT_TECHNOLOGY);

        if (isBlank(vendor) && vendorFromQuery != null && !vendorFromQuery.isBlank() && !unfilteredAllCombos) {
            v = vendorFromQuery;
            log.info("[PmAnalysis] Vendor override from userQuery: vendor={}", v);
        }

        if (!scope.reason.isBlank()) {
            log.info("[PmAnalysis] Dynamic scope resolution: {}", scope.reason);
        }
        return new ResolvedPmParams(d, v, t, dl, nn, g, fromIso, toIso);
    }

    private static String extractVendorFromUserQuery(String userQuery) {
        if (userQuery == null || userQuery.isBlank()) return null;
        String q = userQuery.toLowerCase(Locale.ROOT);
        // Simple deterministic map; extend as needed.
        if (q.contains("cisco")) return "CISCO";
        if (q.contains("juniper")) return "JUNIPER";
        if (q.contains("nokia")) return "NOKIA";
        if (q.contains("ericsson")) return "ERICSSON";
        if (q.contains("huawei")) return "HUAWEI";
        return null;
    }

    private ScopeResolution resolveScopeFromUserQuery(String userQuery) {
        if (userQuery == null || userQuery.isBlank() || this.dataSource == null) {
            return ScopeResolution.empty();
        }
        String q = userQuery.toLowerCase(Locale.ROOT);
        if (q.contains("pan")) {
            String node = matchGeographyName(userQuery, "GEOGRAPHY_L0_NAME");
            return new ScopeResolution(DATA_LEVEL_L0, node,
                    "level=PAN -> dataLevel=" + DATA_LEVEL_L0 + ", nodeName=" + (node == null ? "" : node));
        }
        if (q.contains("region")) {
            String node = matchGeographyName(userQuery, "GEOGRAPHY_L1_NAME");
            return new ScopeResolution(DATA_LEVEL_L1, node,
                    "level=REGION -> dataLevel=" + DATA_LEVEL_L1 + ", nodeName=" + (node == null ? "" : node));
        }
        if (q.contains("state")) {
            String node = matchGeographyName(userQuery, "GEOGRAPHY_L2_NAME");
            return new ScopeResolution(DATA_LEVEL_L2, node,
                    "level=STATE -> dataLevel=" + DATA_LEVEL_L2 + ", nodeName=" + (node == null ? "" : node));
        }
        if (q.contains("city")) {
            String node = matchGeographyName(userQuery, "GEOGRAPHY_L3_NAME");
            return new ScopeResolution(DATA_LEVEL_L3, node,
                    "level=CITY -> dataLevel=" + DATA_LEVEL_L3 + ", nodeName=" + (node == null ? "" : node));
        }
        return ScopeResolution.empty();
    }

    private String matchGeographyName(String userQuery, String columnName) {
        if (isBlank(userQuery) || isBlank(columnName) || this.dataSource == null) {
            return null;
        }
        List<String> names = fetchDistinctAlarmColumnValues(columnName, 5000);
        if (names.isEmpty()) {
            return null;
        }
        String q = userQuery.toLowerCase(Locale.ROOT);
        String best = null;
        for (String n : names) {
            String name = n == null ? "" : n.trim();
            if (name.isBlank()) continue;
            String ln = name.toLowerCase(Locale.ROOT);
            if (q.contains(ln)) {
                if (best == null || name.length() > best.length()) {
                    best = name;
                }
            }
        }
        return best;
    }

    private List<String> fetchDistinctAlarmColumnValues(String requestedColumn, int maxRows) {
        if (this.dataSource == null) return List.of();
        String actualColumn = resolveActualAlarmColumnName(requestedColumn);
        if (actualColumn == null) {
            log.warn("[PmAnalysis] Could not resolve ALARM column '{}'", requestedColumn);
            return List.of();
        }
        String sql = "SELECT DISTINCT " + actualColumn + " FROM ALARM WHERE " + actualColumn
                + " IS NOT NULL AND TRIM(" + actualColumn + ") <> '' LIMIT " + Math.max(1, maxRows);
        LinkedHashSet<String> out = new LinkedHashSet<>();
        try (Connection conn = dataSource.getConnection();
                java.sql.Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String v = rs.getString(1);
                if (v != null && !v.isBlank()) out.add(v.trim());
            }
        }
        catch (SQLException e) {
            log.warn("[PmAnalysis] Failed geography lookup for column '{}': {}", actualColumn, e.getMessage());
            return List.of();
        }
        return List.copyOf(out);
    }

    private String resolveActualAlarmColumnName(String requestedColumn) {
        if (isBlank(requestedColumn) || this.dataSource == null) return null;
        String wanted = requestedColumn.trim().toLowerCase(Locale.ROOT);
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String schema = conn.getSchema();
            try (ResultSet rs = meta.getColumns(null, schema, "ALARM", "%")) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    if (col != null && col.trim().toLowerCase(Locale.ROOT).equals(wanted)) {
                        return col;
                    }
                }
            }
        }
        catch (SQLException e) {
            log.warn("[PmAnalysis] Unable to inspect ALARM metadata: {}", e.getMessage());
        }
        return null;
    }

    private static boolean isReportRequested(String userQuery) {
        if (userQuery == null || userQuery.isBlank()) return false;
        String q = userQuery.toLowerCase(Locale.ROOT);
        return q.contains("report") || q.contains("summary report");
    }

    /**
     * Enforces plain conversational style for non-report asks, even if the model returns
     * markdown/report formatting.
     */
    private static String forceConversationalTone(String text) {
        if (text == null || text.isBlank()) return text;
        String out = text;
        out = HEADING_PREFIX.matcher(out).replaceAll("");
        out = MARKDOWN_BOLD.matcher(out).replaceAll("");
        out = MARKDOWN_CODE.matcher(out).replaceAll("");
        out = BULLET_PREFIX.matcher(out).replaceAll("- ");
        out = out.replaceAll("(?m)^\\s*What to check next\\s*$", "Next checks:");
        out = out.replaceAll("(?m)^\\s*Recommendations\\s*:?", "Next checks:");
        out = out.replaceAll("\\n{3,}", "\n\n").trim();
        // Keep it conversational, but do not truncate content.
        return out;
    }

    private static List<String> extractRequestedKpiTerms(String userQuery) {
        if (userQuery == null || userQuery.isBlank()) return List.of();
        String q = userQuery.toLowerCase(Locale.ROOT);
        LinkedHashSet<String> terms = new LinkedHashSet<>();
        if (!(q.contains("kpi") || q.contains("traffic") || q.contains("availability")
                || q.contains("latency") || q.contains("loss") || q.contains("utilization")
                || q.contains("throughput"))) {
            return List.of();
        }
        if (q.contains("traffic")) {
            terms.add("traffic");
            terms.add("throughput");
            terms.add("bandwidth");
        }
        if (q.contains("availability")) {
            terms.add("availability");
            terms.add("service drop");
            terms.add("drop");
            terms.add("uptime");
        }
        if (q.contains("latency")) {
            terms.add("latency");
            terms.add("delay");
            terms.add("rtt");
        }
        if (q.contains("loss")) {
            terms.add("loss");
            terms.add("packet loss");
            terms.add("discard");
        }
        if (q.contains("utilization")) {
            terms.add("utilization");
            terms.add("cpu");
            terms.add("memory");
        }
        if (q.contains("throughput")) {
            terms.add("throughput");
        }
        return List.copyOf(terms);
    }

    private static boolean matchesAnyTerm(String haystackLower, List<String> terms) {
        if (haystackLower == null || terms == null || terms.isEmpty()) return false;
        for (String term : terms) {
            if (term != null && !term.isBlank() && haystackLower.contains(term.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private Set<String> resolveKpiCodesForTerms(List<String> terms, String domain, String vendor, String technology) {
        if (terms == null || terms.isEmpty() || dataSource == null) return Set.of();
        LinkedHashSet<String> out = new LinkedHashSet<>();
        String sql = "SELECT DISTINCT KPI_CODE, KPI_NAME, DESCRIPTION, KPI_GROUP, FORMULA_COUNTER_INFO "
                + "FROM KPI_FORMULA WHERE (DELETED = 0 OR DELETED IS NULL) "
                + "AND UPPER(DOMAIN)=UPPER(?) AND UPPER(VENDOR)=UPPER(?) AND UPPER(TECHNOLOGY)=UPPER(?)";
        try (Connection conn = dataSource.getConnection();
                java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, domain);
            ps.setString(2, vendor);
            ps.setString(3, technology);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String code = rs.getString("KPI_CODE");
                    String haystack = (safe(rs.getString("KPI_NAME")) + " "
                            + safe(rs.getString("DESCRIPTION")) + " "
                            + safe(rs.getString("KPI_GROUP")) + " "
                            + safe(rs.getString("FORMULA_COUNTER_INFO")) + " "
                            + safe(code)).toLowerCase(Locale.ROOT);
                    if (code != null && !code.isBlank() && matchesAnyTerm(haystack, terms)) {
                        out.add(code.trim());
                    }
                }
            }
        }
        catch (SQLException e) {
            log.warn("[PmAnalysis] Failed KPI term resolution from KPI_FORMULA: {}", e.getMessage());
        }
        log.info("[PmAnalysis] KPI DB resolution terms={} matchedCodes={}", terms, out.size());
        return Set.copyOf(out);
    }

    private static String toJsonArray(Set<String> values) {
        if (values == null || values.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (String v : values) {
            if (v == null || v.isBlank()) continue;
            if (!first) sb.append(",");
            sb.append("\"").append(v.replace("\"", "\\\"")).append("\"");
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static String blankToDefault(String s, String def) {
        return (s == null || s.isBlank()) ? def : s.trim();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private record ResolvedPmParams(String domain, String vendor, String technology, String dataLevel,
            String nodeName, String granularity, String from, String to) {
    }

    private record ScopeResolution(String dataLevel, String nodeName, String reason) {
        private static ScopeResolution empty() {
            return new ScopeResolution(null, null, "");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String pmApiBaseUrl = "http://localhost:8080";
        private org.springaicommunity.nova.pm.analytics.PmAnalyticsEngine analyticsEngine;
        private ChatClient.Builder chatClientBuilder;
        private DataSource dataSource;

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

        public Builder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public PmDataFetchTool build() {
            Assert.notNull(analyticsEngine, "analyticsEngine must not be null");
            Assert.notNull(chatClientBuilder, "chatClientBuilder must not be null");
            return new PmDataFetchTool(pmApiBaseUrl, analyticsEngine, chatClientBuilder, dataSource);
        }
    }
}
