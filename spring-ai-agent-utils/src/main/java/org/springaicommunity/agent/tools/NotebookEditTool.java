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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

/**
 * Spring AI implementation of Claude Code's NotebookEdit tool.
 *
 * <p>Allows AI agents to edit Jupyter notebooks (.ipynb files) by inserting,
 * replacing, or deleting individual cells. Notebook files are standard JSON,
 * so this tool parses the JSON structure, applies targeted edits, and writes
 * the result back to disk.
 *
 * <p>Supported edit modes:
 * <ul>
 *   <li>{@code replace} - Replace the source of an existing cell</li>
 *   <li>{@code insert} - Insert a new cell before the given index</li>
 *   <li>{@code delete} - Remove the cell at the given index</li>
 * </ul>
 *
 * @author Spring AI Community
 */
public class NotebookEditTool {

	private final ObjectMapper objectMapper;

	private NotebookEditTool(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	// @formatter:off
	@Tool(name = "NotebookEdit", description = """
		Edit a Jupyter notebook (.ipynb file) by inserting, replacing, or deleting a cell.

		Jupyter notebooks are JSON files containing an ordered list of cells. Each cell has
		a type (code or markdown) and source content. This tool targets a single cell by
		its 0-based index and applies one of three operations.

		## Parameters

		- notebookPath: Absolute path to the .ipynb file (must exist for replace/delete)
		- cellNumber:   0-based index of the cell to target
		- newSource:    New content for the cell (not required for delete mode)
		- cellType:     'code' or 'markdown' — required only for insert mode (defaults to 'code')
		- editMode:     One of 'replace', 'insert', or 'delete'

		## Edit Modes

		**replace** — overwrite the source of the cell at cellNumber. The cell type is preserved.

		**insert** — insert a new cell BEFORE the cell at cellNumber.
		  - If cellNumber equals the total number of cells, the cell is appended at the end.
		  - Specify cellType to choose 'code' (default) or 'markdown'.

		**delete** — remove the cell at cellNumber entirely.

		## Output Format

		Returns a brief confirmation message. On error, returns an error description.

		## Examples

		Replace cell 2 with new Python code:
		  notebookPath=/path/to/notebook.ipynb, cellNumber=2, newSource="print('hello')", editMode=replace

		Insert a markdown heading before cell 0:
		  notebookPath=/path/to/notebook.ipynb, cellNumber=0, newSource="# My Title", cellType=markdown, editMode=insert

		Delete cell 5:
		  notebookPath=/path/to/notebook.ipynb, cellNumber=5, editMode=delete

		## Notes

		- Cell outputs are cleared when a code cell's source is replaced or a new code cell is inserted.
		- The notebook's kernel and language metadata are preserved unchanged.
		- Always read the notebook (use the Read tool on the .ipynb file) before editing to know the current cell indices.
		""")
	public String notebookEdit( // @formatter:on
			@ToolParam(description = "Absolute path to the .ipynb notebook file") String notebookPath,
			@ToolParam(description = "0-based index of the cell to target") int cellNumber,
			@ToolParam(description = "New source content for the cell. Not needed for delete mode.",
					required = false) String newSource,
			@ToolParam(description = "Cell type for insert mode: 'code' or 'markdown'. Defaults to 'code'.",
					required = false) String cellType,
			@ToolParam(description = "Edit mode: 'replace', 'insert', or 'delete'") String editMode) {

		Path path = Paths.get(notebookPath);

		if (!Files.exists(path)) {
			return "Error: Notebook file not found: " + notebookPath;
		}
		if (!notebookPath.endsWith(".ipynb")) {
			return "Error: File is not a Jupyter notebook (.ipynb): " + notebookPath;
		}

		try {
			String content = Files.readString(path, StandardCharsets.UTF_8);
			ObjectNode notebook = (ObjectNode) this.objectMapper.readTree(content);
			ArrayNode cells = (ArrayNode) notebook.get("cells");

			if (cells == null) {
				return "Error: Notebook has no 'cells' array. It may be malformed.";
			}

			int cellCount = cells.size();

			return switch (editMode.toLowerCase()) {
				case "replace" -> replace(notebook, cells, cellNumber, cellCount, newSource, path);
				case "insert" -> insert(notebook, cells, cellNumber, cellCount, newSource, cellType, path);
				case "delete" -> delete(notebook, cells, cellNumber, cellCount, path);
				default -> "Error: Unknown editMode '" + editMode + "'. Use 'replace', 'insert', or 'delete'.";
			};
		}
		catch (IOException e) {
			return "Error reading or writing notebook: " + e.getMessage();
		}
		catch (ClassCastException e) {
			return "Error: Notebook JSON structure is unexpected (cells is not an array or notebook is not an object).";
		}
	}

	private String replace(ObjectNode notebook, ArrayNode cells, int cellNumber, int cellCount, String newSource,
			Path path) throws IOException {
		if (newSource == null) {
			return "Error: newSource is required for replace mode.";
		}
		if (cellNumber < 0 || cellNumber >= cellCount) {
			return String.format("Error: cellNumber %d is out of range. Notebook has %d cells (0-%d).",
					cellNumber, cellCount, cellCount - 1);
		}

		ObjectNode cell = (ObjectNode) cells.get(cellNumber);
		String type = cell.path("cell_type").asText("code");
		cell.set("source", toSourceArray(newSource));

		if ("code".equals(type)) {
			cell.putArray("outputs");
			cell.putNull("execution_count");
		}

		writeNotebook(notebook, path);
		return String.format("Replaced source of cell %d (%s) in %s.", cellNumber, type, path.getFileName());
	}

	private String insert(ObjectNode notebook, ArrayNode cells, int cellNumber, int cellCount, String newSource,
			String cellType, Path path) throws IOException {
		if (newSource == null) {
			return "Error: newSource is required for insert mode.";
		}
		if (cellNumber < 0 || cellNumber > cellCount) {
			return String.format("Error: cellNumber %d is out of range for insert. Valid range is 0-%d.",
					cellNumber, cellCount);
		}

		String type = (cellType != null && "markdown".equalsIgnoreCase(cellType)) ? "markdown" : "code";
		ObjectNode newCell = this.objectMapper.createObjectNode();
		newCell.put("cell_type", type);
		newCell.set("metadata", this.objectMapper.createObjectNode());
		newCell.set("source", toSourceArray(newSource));

		if ("code".equals(type)) {
			newCell.putArray("outputs");
			newCell.putNull("execution_count");
		}

		// Rebuild cells array with the new cell inserted at the right position
		List<JsonNode> cellList = new ArrayList<>();
		cells.forEach(cellList::add);
		cellList.add(cellNumber, newCell);

		ArrayNode newCells = this.objectMapper.createArrayNode();
		cellList.forEach(newCells::add);
		notebook.set("cells", newCells);

		writeNotebook(notebook, path);
		return String.format("Inserted new %s cell before index %d in %s.", type, cellNumber, path.getFileName());
	}

	private String delete(ObjectNode notebook, ArrayNode cells, int cellNumber, int cellCount,
			Path path) throws IOException {
		if (cellNumber < 0 || cellNumber >= cellCount) {
			return String.format("Error: cellNumber %d is out of range. Notebook has %d cells (0-%d).",
					cellNumber, cellCount, cellCount - 1);
		}

		List<JsonNode> cellList = new ArrayList<>();
		cells.forEach(cellList::add);
		String type = cellList.get(cellNumber).path("cell_type").asText("code");
		cellList.remove(cellNumber);

		ArrayNode newCells = this.objectMapper.createArrayNode();
		cellList.forEach(newCells::add);
		notebook.set("cells", newCells);

		writeNotebook(notebook, path);
		return String.format("Deleted cell %d (%s) from %s. Notebook now has %d cells.",
				cellNumber, type, path.getFileName(), cellList.size());
	}

	/**
	 * Jupyter notebook source fields can be a string or an array of strings (one per
	 * line). We always write as an array for maximum compatibility.
	 */
	private ArrayNode toSourceArray(String source) {
		ArrayNode arr = this.objectMapper.createArrayNode();
		String[] lines = source.split("\n", -1);
		for (int i = 0; i < lines.length; i++) {
			// Each line except the last should end with \n
			arr.add(i < lines.length - 1 ? lines[i] + "\n" : lines[i]);
		}
		return arr;
	}

	private void writeNotebook(ObjectNode notebook, Path path) throws IOException {
		String output = this.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(notebook);
		Files.writeString(path, output, StandardCharsets.UTF_8);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private ObjectMapper objectMapper = new ObjectMapper();

		public Builder objectMapper(ObjectMapper objectMapper) {
			this.objectMapper = objectMapper;
			return this;
		}

		public NotebookEditTool build() {
			return new NotebookEditTool(this.objectMapper);
		}

	}

}
