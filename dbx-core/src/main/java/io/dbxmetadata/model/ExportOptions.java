package io.dbxmetadata.model;

import java.nio.file.Path;
import java.util.Objects;

public final class ExportOptions {

    private final ExportFormat format;
    private final Path outputPath;
    private final boolean prettyPrint;
    private final boolean includeProcedures;
    private final boolean includeTriggers;
    private final boolean includeIndexDetails;
    private final boolean includeComments;
    private final boolean includeViewDefinitions;

    private ExportOptions(Builder builder) {
        this.format = Objects.requireNonNull(builder.format, "Format cannot be null");
        this.outputPath = builder.outputPath;
        this.prettyPrint = builder.prettyPrint;
        this.includeProcedures = builder.includeProcedures;
        this.includeTriggers = builder.includeTriggers;
        this.includeIndexDetails = builder.includeIndexDetails;
        this.includeComments = builder.includeComments;
        this.includeViewDefinitions = builder.includeViewDefinitions;
    }

    public ExportOptions(ExportFormat format, Path outputPath) {
        this(builder().format(format).outputPath(outputPath));
    }

    public ExportFormat getFormat() {
        return format;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public boolean isPrettyPrint() {
        return prettyPrint;
    }

    public boolean isIncludeProcedures() {
        return includeProcedures;
    }

    public boolean isIncludeTriggers() {
        return includeTriggers;
    }

    public boolean isIncludeIndexDetails() {
        return includeIndexDetails;
    }

    public boolean isIncludeComments() {
        return includeComments;
    }

    public boolean isIncludeViewDefinitions() {
        return includeViewDefinitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportOptions that = (ExportOptions) o;
        return prettyPrint == that.prettyPrint &&
               includeProcedures == that.includeProcedures &&
               includeTriggers == that.includeTriggers &&
               includeIndexDetails == that.includeIndexDetails &&
               includeComments == that.includeComments &&
               includeViewDefinitions == that.includeViewDefinitions &&
               format == that.format &&
               Objects.equals(outputPath, that.outputPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, outputPath, prettyPrint, includeProcedures, 
                           includeTriggers, includeIndexDetails, includeComments, 
                           includeViewDefinitions);
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum ExportFormat {
        JSON
    }

    public static final class Builder {
        private ExportFormat format = ExportFormat.JSON;
        private Path outputPath;
        private boolean prettyPrint = true;
        private boolean includeProcedures = true;
        private boolean includeTriggers = true;
        private boolean includeIndexDetails = true;
        private boolean includeComments = true;
        private boolean includeViewDefinitions = true;

        private Builder() {}

        public Builder format(ExportFormat format) {
            this.format = format;
            return this;
        }

        public Builder outputPath(Path outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        public Builder prettyPrint(boolean prettyPrint) {
            this.prettyPrint = prettyPrint;
            return this;
        }

        public Builder includeProcedures(boolean includeProcedures) {
            this.includeProcedures = includeProcedures;
            return this;
        }

        public Builder includeTriggers(boolean includeTriggers) {
            this.includeTriggers = includeTriggers;
            return this;
        }

        public Builder includeIndexDetails(boolean includeIndexDetails) {
            this.includeIndexDetails = includeIndexDetails;
            return this;
        }

        public Builder includeComments(boolean includeComments) {
            this.includeComments = includeComments;
            return this;
        }

        public Builder includeViewDefinitions(boolean includeViewDefinitions) {
            this.includeViewDefinitions = includeViewDefinitions;
            return this;
        }

        public ExportOptions build() {
            return new ExportOptions(this);
        }
    }
}
