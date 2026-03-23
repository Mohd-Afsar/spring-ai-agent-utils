/*
* Copyright 2025 - 2025 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* https://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.springaicommunity.agent.tools;

import java.util.Arrays;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.util.Assert;

/**
 * Spring AI implementation of Claude Code's TodoRead tool.
 *
 * <p>Reads the current task list managed by {@link TodoWriteTool}. Requires a shared
 * {@link TodoStore} instance so both tools operate on the same state.
 *
 * @author Spring AI Community
 */
public class TodoReadTool {

	private final TodoStore todoStore;

	private TodoReadTool(TodoStore todoStore) {
		this.todoStore = todoStore;
	}

	// @formatter:off
	@Tool(name = "TodoRead", description = """
		Read the current task list to check progress on your coding session tasks.

		## When to Use This Tool

		Use this tool to:
		1. Check which tasks are pending, in-progress, or completed
		2. Review overall progress before reporting back to the user
		3. Verify the todo list before deciding what to do next
		4. Confirm a task was properly marked as completed

		## Output Format

		Returns the full task list with status indicators:
		- [ ] pending task
		- [~] in_progress task (currently being worked on)
		- [x] completed task

		If there are no tasks, returns a message indicating the list is empty.

		## When NOT to Use This Tool

		- If you just wrote the todo list with TodoWrite and already know its contents
		- For purely conversational responses where no tasks exist
		""")
	public String todoRead() { // @formatter:on
		TodoWriteTool.Todos todos = this.todoStore.getTodos();

		if (todos == null || todos.todos() == null || todos.todos().length == 0) {
			return "Todo list is empty. No tasks have been created yet.";
		}

		TodoWriteTool.Todos.TodoItem[] items = todos.todos();
		StringBuilder sb = new StringBuilder("Current Todo List:\n\n");

		for (int i = 0; i < items.length; i++) {
			TodoWriteTool.Todos.TodoItem item = items[i];
			String icon = switch (item.status()) {
				case pending -> "[ ]";
				case in_progress -> "[~]";
				case completed -> "[x]";
			};
			sb.append(String.format("%d. %s %s\n", i + 1, icon, item.content()));
		}

		long pending = Arrays.stream(items).filter(t -> t.status() == TodoWriteTool.Todos.Status.pending).count();
		long inProgress = Arrays.stream(items).filter(t -> t.status() == TodoWriteTool.Todos.Status.in_progress).count();
		long completed = Arrays.stream(items).filter(t -> t.status() == TodoWriteTool.Todos.Status.completed).count();

		sb.append(String.format("\nSummary: %d pending, %d in-progress, %d completed (total: %d)",
				pending, inProgress, completed, items.length));

		return sb.toString();
	}

	public static Builder builder(TodoStore todoStore) {
		return new Builder(todoStore);
	}

	public static class Builder {

		private final TodoStore todoStore;

		private Builder(TodoStore todoStore) {
			Assert.notNull(todoStore, "todoStore must not be null");
			this.todoStore = todoStore;
		}

		public TodoReadTool build() {
			return new TodoReadTool(this.todoStore);
		}

	}

}
