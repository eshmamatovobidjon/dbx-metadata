package io.dbxmetadata.model;

import java.nio.file.Path;
import java.util.Objects;

public final class ExportResult {

    private final boolean success;
    private final Path outputPath;
    private final long bytesWritten;
    private final String errorMessage;

    private ExportResult(Builder builder) {
        this.success = builder.success;
        this.outputPath = builder.outputPath;
        this.bytesWritten = builder.bytesWritten;
        this.errorMessage = builder.errorMessage;
    }

    public boolean isSuccess() {
        return success;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportResult that = (ExportResult) o;
        return success == that.success &&
               bytesWritten == that.bytesWritten &&
               Objects.equals(outputPath, that.outputPath) &&
               Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, outputPath, bytesWritten, errorMessage);
    }

    @Override
    public String toString() {
        return "ExportResult{" +
               "success=" + success +
               ", outputPath=" + outputPath +
               ", bytesWritten=" + bytesWritten +
               (errorMessage != null ? ", error='" + errorMessage + '\'' : "") +
               '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ExportResult success(Path outputPath, long bytesWritten) {
        return builder()
                .success(true)
                .outputPath(outputPath)
                .bytesWritten(bytesWritten)
                .build();
    }

    public static ExportResult failure(String errorMessage) {
        return builder()
                .success(false)
                .errorMessage(errorMessage)
                .build();
    }

    public static final class Builder {
        private boolean success;
        private Path outputPath;
        private long bytesWritten;
        private String errorMessage;

        private Builder() {}

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder outputPath(Path outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        public Builder bytesWritten(long bytesWritten) {
            this.bytesWritten = bytesWritten;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public ExportResult build() {
            return new ExportResult(this);
        }
    }
}
