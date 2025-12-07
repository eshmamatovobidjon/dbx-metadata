package io.dbxmetadata.strategy;

import io.dbxmetadata.impl.AbstractMetadataStrategy;

public class GenericJdbcMetadataStrategy extends AbstractMetadataStrategy {

    @Override
    public boolean supports(String databaseProductName) {
        // This is the fallback strategy - always supports
        return true;
    }

    @Override
    public String getVendorName() {
        return "Generic JDBC";
    }

    @Override
    protected boolean shouldIncludeSchema(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return false;
        }
        // Filter common system schemas
        String lower = schemaName.toLowerCase();
        return !lower.equals("information_schema") &&
                !lower.startsWith("pg_") &&
                !lower.equals("sys") &&
                !lower.equals("mysql");
    }
}
