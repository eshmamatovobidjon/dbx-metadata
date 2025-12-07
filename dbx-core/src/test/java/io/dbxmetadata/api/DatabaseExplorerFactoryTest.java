package io.dbxmetadata.api;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.strategy.MetadataStrategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("DatabaseExplorerFactory Tests")
class DatabaseExplorerFactoryTest {

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData dbMetaData;

    @Test
    @DisplayName("Should create explorer for PostgreSQL")
    void shouldCreateExplorerForPostgres() throws SQLException {
        when(connection.getMetaData()).thenReturn(dbMetaData);
        when(dbMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
        when(dbMetaData.getDatabaseProductVersion()).thenReturn("14.0");

        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        assertNotNull(explorer);
        assertEquals("PostgreSQL", explorer.getDatabaseProductName());
        assertEquals("14.0", explorer.getDatabaseProductVersion());
    }

    @Test
    @DisplayName("Should create explorer for MySQL")
    void shouldCreateExplorerForMySQL() throws SQLException {
        when(connection.getMetaData()).thenReturn(dbMetaData);
        when(dbMetaData.getDatabaseProductName()).thenReturn("MySQL");
        when(dbMetaData.getDatabaseProductVersion()).thenReturn("8.0.32");

        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        assertNotNull(explorer);
        assertEquals("MySQL", explorer.getDatabaseProductName());
    }

    @Test
    @DisplayName("Should create explorer for SQL Server")
    void shouldCreateExplorerForSqlServer() throws SQLException {
        when(connection.getMetaData()).thenReturn(dbMetaData);
        when(dbMetaData.getDatabaseProductName()).thenReturn("Microsoft SQL Server");
        when(dbMetaData.getDatabaseProductVersion()).thenReturn("15.0");

        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        assertNotNull(explorer);
        assertEquals("Microsoft SQL Server", explorer.getDatabaseProductName());
    }

    @Test
    @DisplayName("Should create explorer for MariaDB")
    void shouldCreateExplorerForMariaDB() throws SQLException {
        when(connection.getMetaData()).thenReturn(dbMetaData);
        when(dbMetaData.getDatabaseProductName()).thenReturn("MariaDB");
        when(dbMetaData.getDatabaseProductVersion()).thenReturn("10.6.12");

        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        assertNotNull(explorer);
        assertEquals("MariaDB", explorer.getDatabaseProductName());
    }

    @Test
    @DisplayName("Should create explorer for unknown database using generic strategy")
    void shouldCreateExplorerForUnknownDatabase() throws SQLException {
        when(connection.getMetaData()).thenReturn(dbMetaData);
        when(dbMetaData.getDatabaseProductName()).thenReturn("SomeUnknownDB");
        when(dbMetaData.getDatabaseProductVersion()).thenReturn("1.0");

        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        assertNotNull(explorer);
        assertEquals("SomeUnknownDB", explorer.getDatabaseProductName());
    }

    @Test
    @DisplayName("Should throw exception when connection is null")
    void shouldThrowExceptionWhenConnectionIsNull() {
        assertThrows(NullPointerException.class, () ->
                DatabaseExplorerFactory.create((Connection) null));
    }

    @Test
    @DisplayName("Should throw exception when getMetaData fails")
    void shouldThrowExceptionWhenGetMetaDataFails() throws SQLException {
        when(connection.getMetaData()).thenThrow(new SQLException("Connection closed"));

        assertThrows(MetadataExtractionException.class, () ->
                DatabaseExplorerFactory.create(connection));
    }

    @Test
    @DisplayName("Should create explorer with explicit strategy")
    void shouldCreateExplorerWithExplicitStrategy() throws SQLException {
        when(connection.getMetaData()).thenReturn(dbMetaData);
        when(dbMetaData.getDatabaseProductName()).thenReturn("TestDB");
        when(dbMetaData.getDatabaseProductVersion()).thenReturn("1.0");

        MetadataStrategy customStrategy = mock(MetadataStrategy.class);
        when(customStrategy.getVendorName()).thenReturn("CustomDB");

        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection, customStrategy);

        assertNotNull(explorer);
        assertEquals("TestDB", explorer.getDatabaseProductName());
    }
}
