package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TriggerMetadata {

    private final String name;
    private final String tableName;
    private final TriggerTiming timing;
    private final TriggerEvent event;
    private final String definition;
    private final boolean enabled;

    private TriggerMetadata(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "Trigger name cannot be null");
        this.tableName = builder.tableName;
        this.timing = builder.timing;
        this.event = builder.event;
        this.definition = builder.definition;
        this.enabled = builder.enabled;
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return tableName;
    }

    public TriggerTiming getTiming() {
        return timing;
    }

    public TriggerEvent getEvent() {
        return event;
    }

    public String getDefinition() {
        return definition;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TriggerMetadata that = (TriggerMetadata) o;
        return enabled == that.enabled &&
                Objects.equals(name, that.name) &&
                Objects.equals(tableName, that.tableName) &&
                timing == that.timing &&
                event == that.event &&
                Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tableName, timing, event, definition, enabled);
    }

    @Override
    public String toString() {
        return "TriggerMetadata{" +
                "name='" + name + '\'' +
                ", tableName='" + tableName + '\'' +
                ", timing=" + timing +
                ", event=" + event +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String name) {
        return new Builder().name(name);
    }

    public enum TriggerTiming {
        BEFORE,
        AFTER,
        INSTEAD_OF
    }

    public enum TriggerEvent {
        INSERT,
        UPDATE,
        DELETE,
        TRUNCATE
    }

    public static final class Builder {
        private String name;
        private String tableName;
        private TriggerTiming timing;
        private TriggerEvent event;
        private String definition;
        private boolean enabled = true;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder timing(TriggerTiming timing) {
            this.timing = timing;
            return this;
        }

        public Builder event(TriggerEvent event) {
            this.event = event;
            return this;
        }

        public Builder definition(String definition) {
            this.definition = definition;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public TriggerMetadata build() {
            return new TriggerMetadata(this);
        }
    }
}