package io.dbxmetadata;

import io.dbxmetadata.api.DatabaseExplorer;
import io.dbxmetadata.api.DatabaseExplorerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@ConditionalOnClass({DataSource.class, DatabaseExplorer.class})
@EnableConfigurationProperties(DbxMetadataProperties.class)
public class DbxMetadataAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DbxMetadataAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public DatabaseExplorer databaseExplorer(DataSource dataSource, DbxMetadataProperties properties)
            throws SQLException {

        if (!properties.isEnabled()) {
            log.info("DBX Metadata is disabled via configuration");
            return null;
        }

        log.info("Creating DatabaseExplorer bean with auto-configuration");

        try (Connection testConnection = dataSource.getConnection()) {
            log.debug("Successfully tested database connection");
        }

        // Create explorer with a fresh connection
        Connection connection = dataSource.getConnection();
        DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

        log.info("DatabaseExplorer configured for {} {}",
                explorer.getDatabaseProductName(),
                explorer.getDatabaseProductVersion());

        return explorer;
    }
}
