package io.dbxmetadata.integration;

import io.dbxmetadata.api.DatabaseExplorer;
import io.dbxmetadata.api.DatabaseExplorerFactory;
import io.dbxmetadata.model.*;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("H2 Integration Tests")
class H2IntegrationTest {

    private static Connection connection;

    @BeforeAll
    static void setUp() throws Exception {
        // Create H2 in-memory database with test schema
        connection = DriverManager.getConnection(
                "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement stmt = connection.createStatement()) {
            // Create schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS test_schema");
            stmt.execute("SET SCHEMA test_schema");

            // Create tables
            stmt.execute("""
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY AUTO_INCREMENT,
                        email VARCHAR(255) NOT NULL UNIQUE,
                        name VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """);

            stmt.execute("""
                    CREATE TABLE roles (
                        id INTEGER PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(50) NOT NULL UNIQUE
                    )
                    """);

            stmt.execute("""
                    CREATE TABLE user_roles (
                        user_id INTEGER NOT NULL,
                        role_id INTEGER NOT NULL,
                        PRIMARY KEY (user_id, role_id),
                        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                        FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE
                    )
                    """);

            // Create index
            stmt.execute("CREATE INDEX idx_users_email ON users(email)");

            // Create view
            stmt.execute("""
                    CREATE VIEW user_summary AS
                    SELECT u.id, u.name, u.email, COUNT(ur.role_id) as role_count
                    FROM users u
                    LEFT JOIN user_roles ur ON u.id = ur.user_id
                    GROUP BY u.id, u.name, u.email
                    """);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @DisplayName("Should explore database and find schemas")
    void shouldExploreAndFindSchemas() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        assertNotNull(metadata);
        assertNotNull(metadata.getProductName());
        assertTrue(metadata.getProductName().contains("H2"));
        assertFalse(metadata.getSchemas().isEmpty());
    }

    @Test
    @DisplayName("Should find test schema with tables")
    void shouldFindTestSchemaWithTables() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        Optional<SchemaMetadata> testSchema = metadata.findSchema("TEST_SCHEMA");
        assertTrue(testSchema.isPresent());

        SchemaMetadata schema = testSchema.get();
        assertFalse(schema.getTables().isEmpty());

        // Find users table
        Optional<TableMetadata> usersTable = schema.getTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase("USERS"))
                .findFirst();
        assertTrue(usersTable.isPresent());
    }

    @Test
    @DisplayName("Should extract table columns correctly")
    void shouldExtractTableColumnsCorrectly() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        Optional<SchemaMetadata> testSchema = metadata.findSchema("TEST_SCHEMA");
        assertTrue(testSchema.isPresent());

        Optional<TableMetadata> usersTable = testSchema.get().getTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase("USERS"))
                .findFirst();
        assertTrue(usersTable.isPresent());

        TableMetadata users = usersTable.get();
        assertEquals(4, users.getColumns().size());

        // Check ID column
        Optional<ColumnMetadata> idCol = users.getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase("ID"))
                .findFirst();
        assertTrue(idCol.isPresent());
        assertTrue(idCol.get().isPrimaryKey() || idCol.get().isAutoIncrement());

        // Check email column
        Optional<ColumnMetadata> emailCol = users.getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase("EMAIL"))
                .findFirst();
        assertTrue(emailCol.isPresent());
        assertFalse(emailCol.get().isNullable());
    }

    @Test
    @DisplayName("Should extract primary key")
    void shouldExtractPrimaryKey() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        Optional<SchemaMetadata> testSchema = metadata.findSchema("TEST_SCHEMA");
        Optional<TableMetadata> usersTable = testSchema.get().getTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase("USERS"))
                .findFirst();

        assertTrue(usersTable.isPresent());
        PrimaryKeyMetadata pk = usersTable.get().getPrimaryKey();
        assertNotNull(pk);
        assertEquals(1, pk.getColumns().size());
        assertTrue(pk.getColumns().get(0).equalsIgnoreCase("ID"));
    }

    @Test
    @DisplayName("Should extract foreign keys")
    void shouldExtractForeignKeys() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        Optional<SchemaMetadata> testSchema = metadata.findSchema("TEST_SCHEMA");
        Optional<TableMetadata> userRolesTable = testSchema.get().getTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase("USER_ROLES"))
                .findFirst();

        assertTrue(userRolesTable.isPresent());
        assertFalse(userRolesTable.get().getForeignKeys().isEmpty());
        assertEquals(2, userRolesTable.get().getForeignKeys().size());

        // Check FK to users
        Optional<ForeignKeyMetadata> userFk = userRolesTable.get().getForeignKeys().stream()
                .filter(fk -> fk.getReferencedTable().equalsIgnoreCase("USERS"))
                .findFirst();
        assertTrue(userFk.isPresent());
        assertEquals(ForeignKeyMetadata.ForeignKeyAction.CASCADE, userFk.get().getOnDelete());
    }

    @Test
    @DisplayName("Should extract indexes")
    void shouldExtractIndexes() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        Optional<SchemaMetadata> testSchema = metadata.findSchema("TEST_SCHEMA");
        Optional<TableMetadata> usersTable = testSchema.get().getTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase("USERS"))
                .findFirst();

        assertTrue(usersTable.isPresent());
        assertFalse(usersTable.get().getIndexes().isEmpty());

        // Find our custom index
        Optional<IndexMetadata> emailIdx = usersTable.get().getIndexes().stream()
                .filter(i -> i.getName() != null && i.getName().contains("EMAIL"))
                .findFirst();
        assertTrue(emailIdx.isPresent());
    }

    @Test
    @DisplayName("Should extract views")
    void shouldExtractViews() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        DatabaseMetadata metadata = explorer.explore();

        Optional<SchemaMetadata> testSchema = metadata.findSchema("TEST_SCHEMA");
        assertTrue(testSchema.isPresent());

        assertFalse(testSchema.get().getViews().isEmpty());

        Optional<ViewMetadata> summaryView = testSchema.get().getViews().stream()
                .filter(v -> v.getName().equalsIgnoreCase("USER_SUMMARY"))
                .findFirst();
        assertTrue(summaryView.isPresent());
        assertFalse(summaryView.get().getColumns().isEmpty());
    }

    @Test
    @DisplayName("Should list schemas")
    void shouldListSchemas() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        var schemas = explorer.listSchemas();

        assertNotNull(schemas);
        assertFalse(schemas.isEmpty());
        assertTrue(schemas.stream().anyMatch(s -> s.equalsIgnoreCase("TEST_SCHEMA")));
    }

    @Test
    @DisplayName("Should get specific schema")
    void shouldGetSpecificSchema() {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);
        Optional<SchemaMetadata> schema = explorer.getSchema("TEST_SCHEMA");

        assertTrue(schema.isPresent());
        assertEquals("TEST_SCHEMA", schema.get().getName());
    }

    @Test
    @DisplayName("Should export to JSON")
    void shouldExportToJson() throws Exception {
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("metadata", ".json");
        try {
            ExportOptions options = new ExportOptions(
                    ExportOptions.ExportFormat.JSON, tempFile);
            ExportResult result = explorer.export(options);

            assertTrue(result.isSuccess());
            assertTrue(result.getBytesWritten() > 0);
            assertTrue(java.nio.file.Files.exists(tempFile));

            String content = java.nio.file.Files.readString(tempFile);
            assertTrue(content.contains("H2"));
            assertTrue(content.contains("TEST_SCHEMA"));
        } finally {
            java.nio.file.Files.deleteIfExists(tempFile);
        }
    }
}
