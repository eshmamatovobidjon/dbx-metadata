package io.dbxmetadata;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dbx.metadata")
public class DbxMetadataProperties {

    /**
     * Whether DBX Metadata auto-configuration is enabled.
     */
    private boolean enabled = true;

    /**
     * Whether to cache metadata after first exploration.
     */
    private boolean cacheEnabled = false;

    /**
     * Default schema to use if not specified.
     */
    private String defaultSchema;

    /**
     * Whether to include system schemas in exploration.
     */
    private boolean includeSystemSchemas = false;

    /**
     * Whether to include stored procedures in exploration.
     */
    private boolean includeProcedures = true;

    /**
     * Whether to include triggers in exploration.
     */
    private boolean includeTriggers = true;

    /**
     * Whether to include view definitions in exploration.
     */
    private boolean includeViewDefinitions = true;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public boolean isIncludeSystemSchemas() {
        return includeSystemSchemas;
    }

    public void setIncludeSystemSchemas(boolean includeSystemSchemas) {
        this.includeSystemSchemas = includeSystemSchemas;
    }

    public boolean isIncludeProcedures() {
        return includeProcedures;
    }

    public void setIncludeProcedures(boolean includeProcedures) {
        this.includeProcedures = includeProcedures;
    }

    public boolean isIncludeTriggers() {
        return includeTriggers;
    }

    public void setIncludeTriggers(boolean includeTriggers) {
        this.includeTriggers = includeTriggers;
    }

    public boolean isIncludeViewDefinitions() {
        return includeViewDefinitions;
    }

    public void setIncludeViewDefinitions(boolean includeViewDefinitions) {
        this.includeViewDefinitions = includeViewDefinitions;
    }
}
