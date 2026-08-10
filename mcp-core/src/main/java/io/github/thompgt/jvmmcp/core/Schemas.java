package io.github.thompgt.jvmmcp.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Small builders for the JSON Schema maps the MCP SDK expects for tool input and output.
 *
 * <p>Hand-written schema literals drift from the code that reads the arguments. These
 * builders keep the property name in one place, and — more importantly — make the
 * {@code description} of every property a required argument. Property descriptions are read
 * by the model when it constructs a call; an undescribed parameter is a parameter the model
 * will guess at.
 */
public final class Schemas {

    private Schemas() {}

    public static ObjectSchema object() {
        return new ObjectSchema();
    }

    /** Builder for a JSON Schema object with typed, described properties. */
    public static final class ObjectSchema {
        private final Map<String, Object> properties = new LinkedHashMap<>();
        private final List<String> required = new ArrayList<>();

        private ObjectSchema() {}

        public ObjectSchema requiredString(String name, String description) {
            properties.put(name, Map.of("type", "string", "description", description));
            required.add(name);
            return this;
        }

        public ObjectSchema optionalString(String name, String description) {
            properties.put(name, Map.of("type", "string", "description", description));
            return this;
        }

        /**
         * An optional integer with an explicit range. The bounds are part of the schema so
         * the client rejects an out-of-range value before it reaches the policy engine —
         * one less round trip for the model to spend discovering the cap.
         */
        public ObjectSchema optionalInteger(String name, String description, int min, int max) {
            properties.put(
                    name,
                    Map.of(
                            "type", "integer",
                            "description", description,
                            "minimum", min,
                            "maximum", max));
            return this;
        }

        /**
         * An integer with no declared bounds, for a value that is genuinely unbounded — a Kafka
         * offset, a lag, a byte count. Declaring {@code Integer.MAX_VALUE} as the maximum on one
         * of these is not a harmless approximation: structured output is validated against this
         * schema, so the first topic to pass two billion messages would fail every call.
         */
        public ObjectSchema optionalInteger(String name, String description) {
            properties.put(name, Map.of("type", "integer", "description", description));
            return this;
        }

        /**
         * A required string restricted to a fixed set of values.
         *
         * <p>Worth the extra builder over {@link #requiredString} with the values listed in the
         * description: the client rejects a wrong value before the call is made, where a
         * description-only convention costs a round trip and, on a tool that mutates something,
         * makes the model's first attempt at a destructive call the one that fails.
         */
        public ObjectSchema requiredEnum(String name, String description, List<String> values) {
            properties.put(
                    name,
                    Map.of(
                            "type", "string",
                            "description", description,
                            "enum", List.copyOf(values)));
            required.add(name);
            return this;
        }

        public ObjectSchema optionalBoolean(String name, String description) {
            properties.put(name, Map.of("type", "boolean", "description", description));
            return this;
        }

        public ObjectSchema optionalObject(String name, String description) {
            properties.put(name, Map.of("type", "object", "description", description));
            return this;
        }

        public ObjectSchema optionalArrayOfStrings(String name, String description) {
            properties.put(
                    name,
                    Map.of(
                            "type", "array",
                            "description", description,
                            "items", Map.of("type", "string")));
            return this;
        }

        /** A nested array of objects whose element shape is another {@link ObjectSchema}. */
        public ObjectSchema arrayOfObjects(String name, String description, ObjectSchema element) {
            properties.put(
                    name,
                    Map.of(
                            "type", "array",
                            "description", description,
                            "items", element.build()));
            return this;
        }

        /**
         * An array of objects whose keys are not known in advance — query result rows, whose
         * columns depend on the SELECT.
         *
         * <p>Needed because a tool's structured output is validated against its declared
         * output schema. Omitting a field that the tool actually returns makes every call
         * fail validation, which surfaces to the model as an unexplained tool error.
         */
        public ObjectSchema arrayOfDynamicObjects(String name, String description) {
            properties.put(
                    name,
                    Map.of(
                            "type", "array",
                            "description", description,
                            "items", Map.of("type", "object")));
            return this;
        }

        public Map<String, Object> build() {
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("type", "object");
            schema.put("properties", Map.copyOf(properties));
            schema.put("required", List.copyOf(required));
            // MCP clients send only declared arguments; refusing extras turns a typo in a
            // generated call into an immediate, explainable validation error rather than a
            // silently ignored parameter.
            schema.put("additionalProperties", false);
            return Map.copyOf(schema);
        }
    }
}
