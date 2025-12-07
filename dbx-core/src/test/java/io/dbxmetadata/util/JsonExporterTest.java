package io.dbxmetadata.util;

import io.dbxmetadata.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("JsonExporter Tests")
class JsonExporterTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Should export metadata to JSON string")
    void shouldExportMetadataToJsonString() {
        DatabaseMetadata metadata = createSampleMetadata();

        String json = JsonExporter.toJson(metadata);

        assertNotNull(json);
        assertTrue(json.contains("\"productName\" : \"TestDB\""));
        assertTrue(json.contains("\"test_schema\""));
        assertTrue(json.contains("\"users\""));
    }

    @Test
    @DisplayName("Should export metadata to JSON file")
    void shouldExportMetadataToJsonFile() throws Exception {
        DatabaseMetadata metadata = createSampleMetadata();
        Path outputPath = tempDir.resolve("metadata.json");

        ExportOptions options = new ExportOptions(ExportOptions.ExportFormat.JSON, outputPath);
        ExportResult result = JsonExporter.export(metadata, options);

        assertTrue(result.isSuccess());
        assertEquals(outputPath, result.getOutputPath());
        assertTrue(result.getBytesWritten() > 0);
        assertTrue(Files.exists(outputPath));

        String content = Files.readString(outputPath);
        assertTrue(content.contains("TestDB"));
    }

    @Test
    @DisplayName("Should respect pretty print option")
    void shouldRespectPrettyPrintOption() {
        DatabaseMetadata metadata = createSampleMetadata();

        String prettyJson = JsonExporter.toJson(metadata, true);
        String compactJson = JsonExporter.toJson(metadata, false);

        assertTrue(prettyJson.contains("\n"));
        assertTrue(prettyJson.length() > compactJson.length());
    }

    @Test
    @DisplayName("Should filter procedures when disabled")
    void shouldFilterProceduresWhenDisabled() {
        DatabaseMetadata metadata = createMetadataWithProcedures();

        ExportOptions options = ExportOptions.builder()
                .format(ExportOptions.ExportFormat.JSON)
                .includeProcedures(false)
                .build();

        ExportResult result = JsonExporter.export(metadata, options);

        assertTrue(result.isSuccess());
    }

    @Test
    @DisplayName("Should filter comments when disabled")
    void shouldFilterCommentsWhenDisabled() {
        ColumnMetadata col = ColumnMetadata.builder("id")
                .dataType("INTEGER")
                .comment("Primary key column")
                .build();

        TableMetadata table = TableMetadata.builder("test")
                .addColumn(col)
                .comment("Test table comment")
                .build();

        SchemaMetadata schema = SchemaMetadata.builder("test_schema")
                .addTable(table)
                .build();

        DatabaseMetadata metadata = DatabaseMetadata.builder()
                .productName("TestDB")
                .addSchema(schema)
                .build();

        ExportOptions options = ExportOptions.builder()
                .format(ExportOptions.ExportFormat.JSON)
                .includeComments(false)
                .build();

        ExportResult result = JsonExporter.export(metadata, options);
        assertTrue(result.isSuccess());
    }

    @Test
    @DisplayName("Should handle null output path for in-memory export")
    void shouldHandleNullOutputPathForInMemoryExport() {
        DatabaseMetadata metadata = createSampleMetadata();

        ExportOptions options = ExportOptions.builder()
                .format(ExportOptions.ExportFormat.JSON)
                .outputPath(null)
                .build();

        ExportResult result = JsonExporter.export(metadata, options);

        assertTrue(result.isSuccess());
        assertNull(result.getOutputPath());
        assertTrue(result.getBytesWritten() > 0);
    }

    @Test
    @DisplayName("Should include timestamp in exported JSON")
    void shouldIncludeTimestampInExportedJson() {
        Instant now = Instant.now();
        DatabaseMetadata metadata = DatabaseMetadata.builder()
                .productName("TestDB")
                .extractedAt(now)
                .build();

        String json = JsonExporter.toJson(metadata);

        assertTrue(json.contains("extractedAt"));
    }

    private DatabaseMetadata createSampleMetadata() {
        ColumnMetadata idCol = ColumnMetadata.builder("id")
                .dataType("INTEGER")
                .primaryKey(true)
                .build();

        ColumnMetadata nameCol = ColumnMetadata.builder("name")
                .dataType("VARCHAR")
                .size(100)
                .build();

        TableMetadata table = TableMetadata.builder("users")
                .addColumn(idCol)
                .addColumn(nameCol)
                .primaryKey(PrimaryKeyMetadata.builder()
                        .name("pk_users")
                        .addColumn("id")
                        .build())
                .build();

        SchemaMetadata schema = SchemaMetadata.builder("test_schema")
                .addTable(table)
                .build();

        return DatabaseMetadata.builder()
                .productName("TestDB")
                .productVersion("1.0")
                .addSchema(schema)
                .build();
    }

    private DatabaseMetadata createMetadataWithProcedures() {
        ProcedureMetadata proc = ProcedureMetadata.builder("get_user")
                .type(ProcedureMetadata.ProcedureType.FUNCTION)
                .returnType("INTEGER")
                .addParameter("user_id", "INTEGER", ProcedureMetadata.ParameterMode.IN, 1)
                .build();

        SchemaMetadata schema = SchemaMetadata.builder("test_schema")
                .addProcedure(proc)
                .build();

        return DatabaseMetadata.builder()
                .productName("TestDB")
                .addSchema(schema)
                .build();
    }
}
