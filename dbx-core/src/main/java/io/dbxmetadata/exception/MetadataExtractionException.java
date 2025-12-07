package io.dbxmetadata.exception;

import java.sql.SQLException;

public class MetadataExtractionException extends RuntimeException {

    private final String operation;
    private final String objectName;
    private final String vendorErrorCode;

    public MetadataExtractionException(String message) {
        super(message);
        this.operation = null;
        this.objectName = null;
        this.vendorErrorCode = null;
    }

    public MetadataExtractionException(String message, Throwable cause) {
        super(message, cause);
        this.operation = null;
        this.objectName = null;
        if (cause instanceof SQLException sqlEx) {
            this.vendorErrorCode = String.valueOf(sqlEx.getErrorCode());
        } else {
            this.vendorErrorCode = null;
        }
    }

    public MetadataExtractionException(String message, String operation, String objectName, Throwable cause) {
        super(formatMessage(message, operation, objectName), cause);
        this.operation = operation;
        this.objectName = objectName;
        if (cause instanceof SQLException sqlEx) {
            this.vendorErrorCode = String.valueOf(sqlEx.getErrorCode());
        } else {
            this.vendorErrorCode = null;
        }
    }

    private static String formatMessage(String message, String operation, String objectName) {
        StringBuilder sb = new StringBuilder(message);
        if (operation != null) {
            sb.append(" [operation=").append(operation);
            if (objectName != null) {
                sb.append(", object=").append(objectName);
            }
            sb.append("]");
        }
        return sb.toString();
    }

    public String getOperation() {
        return operation;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getVendorErrorCode() {
        return vendorErrorCode;
    }

    public boolean isPermissionError() {
        Throwable cause = getCause();
        if (cause instanceof SQLException sqlEx) {
            String sqlState = sqlEx.getSQLState();
            if (sqlState != null) {
                // Common SQL states for permission denied
                return sqlState.startsWith("42") || // Syntax/access errors
                        sqlState.equals("28000") ||  // Invalid authorization
                        sqlState.startsWith("28");   // Authorization errors
            }
        }
        return false;
    }
}
