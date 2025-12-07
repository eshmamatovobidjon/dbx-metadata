package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents the result of permission checks during metadata extraction.
 * Used to track which operations succeeded or failed due to permission issues.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PermissionResult {

    private final boolean fullAccess;
    private final List<String> accessibleObjects;
    private final List<String> deniedObjects;
    private final List<String> warnings;

    private PermissionResult(Builder builder) {
        this.fullAccess = builder.fullAccess;
        this.accessibleObjects = Collections.unmodifiableList(new ArrayList<>(builder.accessibleObjects));
        this.deniedObjects = Collections.unmodifiableList(new ArrayList<>(builder.deniedObjects));
        this.warnings = Collections.unmodifiableList(new ArrayList<>(builder.warnings));
    }

    public boolean isFullAccess() {
        return fullAccess;
    }

    public List<String> getAccessibleObjects() {
        return accessibleObjects;
    }

    public List<String> getDeniedObjects() {
        return deniedObjects;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PermissionResult that = (PermissionResult) o;
        return fullAccess == that.fullAccess &&
               Objects.equals(accessibleObjects, that.accessibleObjects) &&
               Objects.equals(deniedObjects, that.deniedObjects) &&
               Objects.equals(warnings, that.warnings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullAccess, accessibleObjects, deniedObjects, warnings);
    }

    @Override
    public String toString() {
        return "PermissionResult{" +
               "fullAccess=" + fullAccess +
               ", accessible=" + accessibleObjects.size() +
               ", denied=" + deniedObjects.size() +
               ", warnings=" + warnings.size() +
               '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static PermissionResult fullAccess() {
        return builder().fullAccess(true).build();
    }

    public static final class Builder {
        private boolean fullAccess = true;
        private List<String> accessibleObjects = new ArrayList<>();
        private List<String> deniedObjects = new ArrayList<>();
        private List<String> warnings = new ArrayList<>();

        private Builder() {}

        public Builder fullAccess(boolean fullAccess) {
            this.fullAccess = fullAccess;
            return this;
        }

        public Builder accessibleObjects(List<String> accessibleObjects) {
            this.accessibleObjects = new ArrayList<>(accessibleObjects);
            return this;
        }

        public Builder addAccessibleObject(String object) {
            this.accessibleObjects.add(object);
            return this;
        }

        public Builder deniedObjects(List<String> deniedObjects) {
            this.deniedObjects = new ArrayList<>(deniedObjects);
            this.fullAccess = false;
            return this;
        }

        public Builder addDeniedObject(String object) {
            this.deniedObjects.add(object);
            this.fullAccess = false;
            return this;
        }

        public Builder warnings(List<String> warnings) {
            this.warnings = new ArrayList<>(warnings);
            return this;
        }

        public Builder addWarning(String warning) {
            this.warnings.add(warning);
            return this;
        }

        public PermissionResult build() {
            return new PermissionResult(this);
        }
    }
}
