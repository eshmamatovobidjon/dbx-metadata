package io.dbxmetadata.strategy;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.impl.AbstractMetadataStrategy;
import io.dbxmetadata.model.ColumnMetadata;
import io.dbxmetadata.model.ProcedureMetadata;
import io.dbxmetadata.model.TriggerMetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PostgresMetadataStrategy extends AbstractMetadataStrategy {

    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
            "pg_catalog", "information_schema", "pg_toast", "pg_temp_1", "pg_toast_temp_1"
    );

    @Override
    public boolean supports(String databaseProductName) {
        return databaseProductName != null &&
                databaseProductName.toLowerCase().contains("postgresql");
    }

    @Override
    public String getVendorName() {
        return "PostgreSQL";
    }

    @Override
    protected boolean shouldIncludeSchema(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return false;
        }
        return !SYSTEM_SCHEMAS.contains(schemaName.toLowerCase()) &&
                !schemaName.startsWith("pg_");
    }

    @Override
    protected String extractTableComment(Connection connection, String catalog, String schema, String tableName) {
        String sql = """
                SELECT obj_description(c.oid) AS comment
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ?
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("comment");
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract table comment for {}.{}: {}", schema, tableName, e.getMessage());
        }

        return null;
    }

    @Override
    protected void enhanceColumnComments(Connection connection, String catalog, String schema,
                                         String tableName, List<ColumnMetadata> columns) {

        String sql = """
                SELECT a.attname AS column_name, 
                       col_description(c.oid, a.attnum) AS comment
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                WHERE n.nspname = ? 
                  AND c.relname = ?
                  AND a.attnum > 0 
                  AND NOT a.attisdropped
                  AND col_description(c.oid, a.attnum) IS NOT NULL
                """;

        Map<String, String> comments = new HashMap<>();

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    comments.put(rs.getString("column_name"), rs.getString("comment"));
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract column comments for {}.{}: {}", schema, tableName, e.getMessage());
            return;
        }

        // Update columns with comments (rebuild immutable objects)
        if (!comments.isEmpty()) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnMetadata col = columns.get(i);
                String comment = comments.get(col.getName());
                if (comment != null && (col.getComment() == null || col.getComment().isEmpty())) {
                    columns.set(i, ColumnMetadata.builder()
                            .name(col.getName())
                            .dataType(col.getDataType())
                            .size(col.getSize())
                            .precision(col.getPrecision())
                            .scale(col.getScale())
                            .nullable(col.isNullable())
                            .primaryKey(col.isPrimaryKey())
                            .autoIncrement(col.isAutoIncrement())
                            .defaultValue(col.getDefaultValue())
                            .comment(comment)
                            .ordinalPosition(col.getOrdinalPosition())
                            .build());
                }
            }
        }
    }

    @Override
    protected String extractViewDefinition(Connection connection, String catalog, String schema, String viewName) {
        String sql = """
                SELECT definition 
                FROM pg_views 
                WHERE schemaname = ? AND viewname = ?
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, viewName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("definition");
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract view definition for {}.{}: {}", schema, viewName, e.getMessage());
        }

        return null;
    }

    @Override
    public List<TriggerMetadata> extractTriggers(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<TriggerMetadata> triggers = new ArrayList<>();

        String sql = """
                SELECT t.tgname AS trigger_name,
                       c.relname AS table_name,
                       CASE 
                           WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
                           WHEN t.tgtype & 64 = 64 THEN 'INSTEAD_OF'
                           ELSE 'AFTER'
                       END AS timing,
                       CASE
                           WHEN t.tgtype & 4 = 4 THEN 'INSERT'
                           WHEN t.tgtype & 8 = 8 THEN 'DELETE'
                           WHEN t.tgtype & 16 = 16 THEN 'UPDATE'
                           ELSE 'UNKNOWN'
                       END AS event,
                       t.tgenabled != 'D' AS enabled,
                       pg_get_triggerdef(t.oid) AS definition
                FROM pg_trigger t
                JOIN pg_class c ON c.oid = t.tgrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ?
                  AND NOT t.tgisinternal
                ORDER BY c.relname, t.tgname
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TriggerMetadata.TriggerTiming timing = switch (rs.getString("timing")) {
                        case "BEFORE" -> TriggerMetadata.TriggerTiming.BEFORE;
                        case "INSTEAD_OF" -> TriggerMetadata.TriggerTiming.INSTEAD_OF;
                        default -> TriggerMetadata.TriggerTiming.AFTER;
                    };

                    TriggerMetadata.TriggerEvent event = switch (rs.getString("event")) {
                        case "INSERT" -> TriggerMetadata.TriggerEvent.INSERT;
                        case "DELETE" -> TriggerMetadata.TriggerEvent.DELETE;
                        case "UPDATE" -> TriggerMetadata.TriggerEvent.UPDATE;
                        default -> TriggerMetadata.TriggerEvent.INSERT;
                    };

                    triggers.add(TriggerMetadata.builder()
                            .name(rs.getString("trigger_name"))
                            .tableName(rs.getString("table_name"))
                            .timing(timing)
                            .event(event)
                            .enabled(rs.getBoolean("enabled"))
                            .definition(rs.getString("definition"))
                            .build());
                }
            }
        } catch (SQLException e) {
            log.warn("Could not extract triggers for schema {}: {}", schema, e.getMessage());
            addWarning("Failed to extract triggers for schema " + schema + ": " + e.getMessage());
        }

        return triggers;
    }

    @Override
    protected List<TriggerMetadata> extractTriggersForTable(Connection connection, String catalog,
                                                            String schema, String tableName) throws MetadataExtractionException {
        List<TriggerMetadata> triggers = new ArrayList<>();

        String sql = """
                SELECT t.tgname AS trigger_name,
                       CASE 
                           WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
                           WHEN t.tgtype & 64 = 64 THEN 'INSTEAD_OF'
                           ELSE 'AFTER'
                       END AS timing,
                       CASE
                           WHEN t.tgtype & 4 = 4 THEN 'INSERT'
                           WHEN t.tgtype & 8 = 8 THEN 'DELETE'
                           WHEN t.tgtype & 16 = 16 THEN 'UPDATE'
                           ELSE 'UNKNOWN'
                       END AS event,
                       t.tgenabled != 'D' AS enabled,
                       pg_get_triggerdef(t.oid) AS definition
                FROM pg_trigger t
                JOIN pg_class c ON c.oid = t.tgrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ?
                  AND NOT t.tgisinternal
                ORDER BY t.tgname
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TriggerMetadata.TriggerTiming timing = switch (rs.getString("timing")) {
                        case "BEFORE" -> TriggerMetadata.TriggerTiming.BEFORE;
                        case "INSTEAD_OF" -> TriggerMetadata.TriggerTiming.INSTEAD_OF;
                        default -> TriggerMetadata.TriggerTiming.AFTER;
                    };

                    TriggerMetadata.TriggerEvent event = switch (rs.getString("event")) {
                        case "INSERT" -> TriggerMetadata.TriggerEvent.INSERT;
                        case "DELETE" -> TriggerMetadata.TriggerEvent.DELETE;
                        case "UPDATE" -> TriggerMetadata.TriggerEvent.UPDATE;
                        default -> TriggerMetadata.TriggerEvent.INSERT;
                    };

                    triggers.add(TriggerMetadata.builder()
                            .name(rs.getString("trigger_name"))
                            .tableName(tableName)
                            .timing(timing)
                            .event(event)
                            .enabled(rs.getBoolean("enabled"))
                            .definition(rs.getString("definition"))
                            .build());
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract triggers for table {}.{}: {}", schema, tableName, e.getMessage());
        }

        return triggers;
    }

    @Override
    public List<ProcedureMetadata> extractProcedures(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<ProcedureMetadata> procedures = new ArrayList<>();

        // Query for functions and procedures in PostgreSQL
        String sql = """
                SELECT p.proname AS name,
                       CASE p.prokind
                           WHEN 'f' THEN 'FUNCTION'
                           WHEN 'p' THEN 'PROCEDURE'
                           ELSE 'FUNCTION'
                       END AS type,
                       pg_get_function_result(p.oid) AS return_type,
                       pg_get_functiondef(p.oid) AS definition,
                       d.description AS comment
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                LEFT JOIN pg_description d ON d.objoid = p.oid AND d.classoid = 'pg_proc'::regclass
                WHERE n.nspname = ?
                  AND p.prokind IN ('f', 'p')
                ORDER BY p.proname
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ProcedureMetadata.ProcedureType type = "PROCEDURE".equals(rs.getString("type"))
                            ? ProcedureMetadata.ProcedureType.PROCEDURE
                            : ProcedureMetadata.ProcedureType.FUNCTION;

                    procedures.add(ProcedureMetadata.builder()
                            .name(rs.getString("name"))
                            .type(type)
                            .returnType(rs.getString("return_type"))
                            .definition(rs.getString("definition"))
                            .comment(rs.getString("comment"))
                            .build());
                }
            }
        } catch (SQLException e) {
            // Fall back to JDBC standard method
            log.debug("PostgreSQL-specific procedure extraction failed, falling back to JDBC: {}", e.getMessage());
            return super.extractProcedures(connection, catalog, schema);
        }

        return procedures;
    }
}
