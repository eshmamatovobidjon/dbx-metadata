package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ProcedureMetadata {

    private final String name;
    private final ProcedureType type;
    private final List<ParameterMetadata> parameters;
    private final String returnType;
    private final String definition;
    private final String comment;

    private ProcedureMetadata(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "Procedure name cannot be null");
        this.type = builder.type;
        this.parameters = List.copyOf(builder.parameters);
        this.returnType = builder.returnType;
        this.definition = builder.definition;
        this.comment = builder.comment;
    }

    public String getName() {
        return name;
    }

    public ProcedureType getType() {
        return type;
    }

    public List<ParameterMetadata> getParameters() {
        return parameters;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getDefinition() {
        return definition;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcedureMetadata that = (ProcedureMetadata) o;
        return Objects.equals(name, that.name) &&
                type == that.type &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(definition, that.definition) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, parameters, returnType, definition, comment);
    }

    @Override
    public String toString() {
        return "ProcedureMetadata{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", parameters=" + parameters.size() +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String name) {
        return new Builder().name(name);
    }

    public enum ProcedureType {
        PROCEDURE,
        FUNCTION
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ParameterMetadata(String name, String dataType, ParameterMode mode, int position) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParameterMetadata that = (ParameterMetadata) o;
            return position == that.position &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(dataType, that.dataType) &&
                    mode == that.mode;
        }

        @Override
        public String toString() {
                return mode + " " + name + " " + dataType;
            }
    }

    public enum ParameterMode {
        IN,
        OUT,
        INOUT,
        RETURN
    }

    public static final class Builder {
        private String name;
        private ProcedureType type = ProcedureType.PROCEDURE;
        private List<ParameterMetadata> parameters = new ArrayList<>();
        private String returnType;
        private String definition;
        private String comment;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(ProcedureType type) {
            this.type = type;
            return this;
        }

        public Builder parameters(List<ParameterMetadata> parameters) {
            this.parameters = new ArrayList<>(parameters);
            return this;
        }

        public Builder addParameter(String name, String dataType, ParameterMode mode, int position) {
            this.parameters.add(new ParameterMetadata(name, dataType, mode, position));
            return this;
        }

        public Builder returnType(String returnType) {
            this.returnType = returnType;
            return this;
        }

        public Builder definition(String definition) {
            this.definition = definition;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public ProcedureMetadata build() {
            return new ProcedureMetadata(this);
        }
    }
}
