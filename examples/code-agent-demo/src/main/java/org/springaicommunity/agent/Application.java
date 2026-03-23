package org.springaicommunity.agent;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.springaicommunity.agent.tools.BraveWebSearchTool;
import org.springaicommunity.agent.tools.DatabaseTools;
import org.springaicommunity.agent.tools.FileSystemTools;
import org.springaicommunity.agent.tools.GlobTool;
import org.springaicommunity.agent.tools.GrepTool;
import org.springaicommunity.agent.tools.NotebookEditTool;
import org.springaicommunity.agent.tools.ShellTools;
import org.springaicommunity.agent.tools.SkillsTool;
import org.springaicommunity.agent.tools.SmartWebFetchTool;
import org.springaicommunity.agent.tools.TodoReadTool;
import org.springaicommunity.agent.tools.TodoStore;
import org.springaicommunity.agent.tools.TodoWriteTool;
import org.springaicommunity.agent.utils.AgentEnvironment;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.ToolCallAdvisor;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

@SpringBootApplication
public class Application {

	/** Max retries on HTTP 429 before giving up and telling the user to wait. */
	private static final int MAX_RETRIES = 3;

	/**
	 * Hard cap on how long we wait between retries.
	 * Groq sometimes suggests 600+ seconds — we never wait that long.
	 * We cap at 5s and tell the user to try again if still limited.
	 */
	private static final int MAX_WAIT_SECONDS = 5;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner(ChatClient.Builder chatClientBuilder,
			@Value("${BRAVE_API_KEY:#{null}}") String braveApiKey,
			@Value("classpath:/prompt/MAIN_AGENT_SYSTEM_PROMPT_V2.md") Resource systemPrompt,
			@Value("${agent.model.knowledge.cutoff:Unknown}") String agentModelKnowledgeCutoff,
			@Value("${agent.model:Unknown}") String agentModel,
			@Value("${agent.skills.paths}") List<Resource> skillPaths,
			@Autowired(required = false) ToolCallbackProvider mcpToolCallbackProvider,
			@Autowired(required = false) DataSource dataSource) {

		return args -> {

			TodoStore todoStore = new TodoStore();

			ChatClient.Builder builder = chatClientBuilder
				.defaultSystem(p -> p.text(systemPrompt)
					.param(AgentEnvironment.ENVIRONMENT_INFO_KEY, AgentEnvironment.info())
					.param(AgentEnvironment.GIT_STATUS_KEY, AgentEnvironment.gitStatus())
					.param(AgentEnvironment.AGENT_MODEL_KEY, agentModel)
					.param(AgentEnvironment.AGENT_MODEL_KNOWLEDGE_CUTOFF_KEY, agentModelKnowledgeCutoff));

			if (mcpToolCallbackProvider != null) {
				builder.defaultToolCallbacks(mcpToolCallbackProvider);
			}
			if (StringUtils.hasText(braveApiKey)) {
				builder.defaultTools(BraveWebSearchTool.builder(braveApiKey).resultCount(15).build());
			}

			builder.defaultToolCallbacks(SkillsTool.builder().addSkillsResources(skillPaths).build());
			builder.defaultTools(
				TodoWriteTool.builder().todoEventHandler(todoStore).build(),
				TodoReadTool.builder(todoStore).build());
			builder.defaultTools(
				ShellTools.builder().build(),
				FileSystemTools.builder().build(),
				GlobTool.builder().build(),
				GrepTool.builder().build(),
				NotebookEditTool.builder().build(),
				SmartWebFetchTool.builder(chatClientBuilder.clone().build()).build());

			if (dataSource != null) {
				builder.defaultTools(DatabaseTools.builder(dataSource).maxRows(50).build());
			}

			ChatClient chatClient = builder
				.defaultAdvisors(
					ToolCallAdvisor.builder().conversationHistoryEnabled(false).build(),
					MessageChatMemoryAdvisor
						.builder(MessageWindowChatMemory.builder().maxMessages(20).build())
						.order(Ordered.HIGHEST_PRECEDENCE + 1000)
						.build())
				.build();

			System.out.println("\nI am your Claude-like AI assistant powered by Spring AI.");
			System.out.print("Tools: Bash, Read, Write, Edit, Glob, Grep, TodoWrite, TodoRead, NotebookEdit, WebFetch, Skill");
			if (StringUtils.hasText(braveApiKey)) System.out.print(", WebSearch");
			if (dataSource != null) System.out.print(", DbInfo, DbListTables, DbDescribeTable, DbQuery, DbSample, DbQueryPaged");
			System.out.println("\n");

			try (Scanner scanner = new Scanner(System.in)) {
				while (true) {
					System.out.print("\n> USER: ");
					String input = scanner.nextLine();
					if (input.isBlank()) continue;

					String response = callWithRetry(chatClient, input);
					System.out.println("\n> ASSISTANT: " + response);
				}
			}
		};
	}

	/**
	 * Calls the chat model with a live progress indicator and retries on HTTP 429.
	 *
	 * Why .call() instead of .stream():
	 *   Spring AI SNAPSHOT's ToolCallAdvisor does not properly chain multi-round
	 *   tool calls through a streaming Flux. The stream completes after the model's
	 *   first text chunk, before any tool is executed. Using .call() ensures the full
	 *   tool-call loop (schema exploration → SQL query → analysis) completes before
	 *   returning.
	 *
	 * On 429 rate limit:
	 *   Groq sometimes suggests waiting 600+ seconds. We cap at MAX_WAIT_SECONDS
	 *   and retry up to MAX_RETRIES times. If still limited, we tell the user to
	 *   wait ~1 minute and try a narrower query.
	 */
	private static String callWithRetry(ChatClient chatClient, String input) throws InterruptedException {
		int waitSec = 2;

		for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

			// Background thread: shows elapsed time so the user knows work is in progress
			long startMs = System.currentTimeMillis();
			AtomicBoolean done = new AtomicBoolean(false);
			Thread progress = new Thread(() -> {
				try {
					while (!done.get()) {
						long elapsed = (System.currentTimeMillis() - startMs) / 1000;
						System.out.printf("\rWorking... %ds  ", elapsed);
						System.out.flush();
						Thread.sleep(1000);
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				// Clear the progress line
				System.out.print("\r                    \r");
				System.out.flush();
			});
			progress.setDaemon(true);
			progress.start();

			try {
				String result = chatClient.prompt(input).call().content();
				done.set(true);
				progress.join();
				return result;
			}
			catch (Exception e) {
				done.set(true);
				progress.join();

				String msg = e.getMessage() != null ? e.getMessage() : "";
				boolean isRateLimit = msg.contains("429") || msg.contains("rate_limit_exceeded")
						|| msg.contains("Rate limit");

				if (isRateLimit && attempt < MAX_RETRIES) {
					int capped = Math.min(waitSec, MAX_WAIT_SECONDS);
					System.out.printf("[Rate limited — waiting %ds before retry %d/%d]%n",
							capped, attempt, MAX_RETRIES);
					Thread.sleep(capped * 1000L);
					waitSec = Math.min(waitSec * 2, MAX_WAIT_SECONDS);
				}
				else if (isRateLimit) {
					return "[Rate limit not resolved after " + MAX_RETRIES + " retries. "
							+ "Your token quota for this minute is exhausted. "
							+ "Wait ~1 minute then try again with a more specific query "
							+ "(e.g. name a specific table or region instead of asking for everything at once).]";
				}
				else {
					return "[Error: " + msg + "]";
				}
			}
		}

		return "[Request could not complete — please try again.]";
	}

}
