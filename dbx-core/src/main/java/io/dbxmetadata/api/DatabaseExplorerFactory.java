package io.dbxmetadata.api;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.impl.DefaultDatabaseExplorer;
import io.dbxmetadata.strategy.GenericJdbcMetadataStrategy;
import io.dbxmetadata.strategy.MetadataStrategy;
import io.dbxmetadata.strategy.MsSqlMetadataStrategy;
import io.dbxmetadata.strategy.MySqlMetadataStrategy;
import io.dbxmetadata.strategy.PostgresMetadataStrategy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class DatabaseExplorerFactory {
    private static final List<MetadataStrategy> STRATEGIES = new ArrayList<>();

    static {
        // Register built-in strategies
        registerStrategy(new PostgresMetadataStrategy());
        registerStrategy(new MySqlMetadataStrategy());
        registerStrategy(new MsSqlMetadataStrategy());
        registerStrategy(new GenericJdbcMetadataStrategy()); // Fallback - always last
    }

    private DatabaseExplorerFactory() {
        // Utility class - no instantiation
    }

    public static DatabaseExplorer create(Connection connection) {
        Objects.requireNonNull(connection, "Connection cannot be null");

        try {
            DatabaseMetaData dbMetaData = connection.getMetaData();
            String productName = dbMetaData.getDatabaseProductName();
            String productVersion = dbMetaData.getDatabaseProductVersion();
            MetadataStrategy strategy = findStrategy(productName);
            return new DefaultDatabaseExplorer(connection, strategy, productName, productVersion);
        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to detect database vendor", e);
        }
    }

    public static DatabaseExplorer create(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "DataSource cannot be null");

        try {
            Connection connection = dataSource.getConnection();
            return create(connection);
        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to obtain connection from DataSource", e);
        }
    }

    public static DatabaseExplorer create(Connection connection, MetadataStrategy strategy) {
        Objects.requireNonNull(connection, "Connection cannot be null");
        Objects.requireNonNull(strategy, "Strategy cannot be null");

        try {
            DatabaseMetaData dbMetaData = connection.getMetaData();
            String productName = dbMetaData.getDatabaseProductName();
            String productVersion = dbMetaData.getDatabaseProductVersion();

            return new DefaultDatabaseExplorer(connection, strategy, productName, productVersion);

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to get database metadata", e);
        }
    }

    public static synchronized void registerStrategy(MetadataStrategy strategy) {
        Objects.requireNonNull(strategy, "Strategy cannot be null");

        // Remove existing strategy for same vendor
        STRATEGIES.removeIf(s -> s.getVendorName().equals(strategy.getVendorName()));

        // Add at the beginning (before fallback)
        if (strategy instanceof GenericJdbcMetadataStrategy) {
            STRATEGIES.add(strategy); // Fallback goes last
        } else {
            STRATEGIES.add(0, strategy); // Custom strategies go first
        }
    }

    public static synchronized List<MetadataStrategy> getStrategies() {
        return List.copyOf(STRATEGIES);
    }

    private static synchronized MetadataStrategy findStrategy(String productName) {
        String normalizedName = productName != null ? productName.toLowerCase(Locale.ROOT) : "";

        for (MetadataStrategy strategy : STRATEGIES) {
            if (strategy.supports(normalizedName)) {
                return strategy;
            }
        }

        // Should never happen since GenericJdbcMetadataStrategy supports all
        throw new MetadataExtractionException("No strategy found for database: " + productName);
    }
}
