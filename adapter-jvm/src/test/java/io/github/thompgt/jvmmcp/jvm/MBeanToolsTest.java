package io.github.thompgt.jvmmcp.jvm;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.thompgt.jvmmcp.core.BridgeTool;
import io.github.thompgt.jvmmcp.core.ToolOutcome;
import io.github.thompgt.jvmmcp.policy.AccessMode;
import io.github.thompgt.jvmmcp.policy.AuditSink;
import io.github.thompgt.jvmmcp.policy.PolicyProfile;
import io.github.thompgt.jvmmcp.policy.Redactor;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercised against this JVM's own platform MBeans.
 *
 * <p>No container and no fixture process, which is the one genuine advantage of writing this
 * adapter in Java: the backend under test is the test runner. The platform beans are also a
 * better fixture than anything hand-built would be — {@code HeapMemoryUsage} is a real composite,
 * {@code SystemProperties} a real tabular value with hundreds of rows, and {@code InputArguments}
 * a real array, so the conversion is tested against the shapes it will actually meet rather than
 * against a mock's idea of them.
 */
class MBeanToolsTest {

    private static final String MEMORY = "java.lang:type=Memory";
    private static final String RUNTIME = "java.lang:type=Runtime";

    private JvmAdapter adapter;

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.close();
        }
    }

    private JvmAdapter adapter(PolicyProfile profile) {
        adapter = new JvmAdapter(JvmTargetHandle.embedded("self"), profile, AuditSink.noop());
        return adapter;
    }

    private static PolicyProfile.Builder policy() {
        return PolicyProfile.builder("self")
                .mode(AccessMode.READ_ONLY)
                .allowRead("java.lang:*")
                .maxRows(200)
                .maxResultBytes(256_000L);
    }

    private BridgeTool tool(JvmAdapter target, String name) {
        return target.tools().stream()
                .filter(t -> t.descriptor().name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no tool named " + name));
    }

    private ToolOutcome call(JvmAdapter target, String tool, Map<String, Object> arguments) {
        return tool(target, tool).call(arguments);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(ToolOutcome outcome) {
        return (Map<String, Object>) outcome.structured();
    }

    // AssertJ's map and list assertions do not infer usefully through a wildcard capture, and
    // the alternative — a cast at every assertion — reads worse than two helpers.
    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    @Test
    void theMBeanToolsAreRegistered() {
        assertThat(adapter(policy().build()).tools())
                .extracting(t -> t.descriptor().name())
                .contains("jvm.mbeans", "jvm.attribute");
    }

    @Test
    void listsPlatformMBeansWithTheirAttributeNames() {
        ToolOutcome outcome = call(adapter(policy().build()), "jvm.mbeans", Map.of("pattern", "java.lang:*"));

        assertThat(outcome.error()).isFalse();
        Map<String, Object> result = structured(outcome);
        assertThat(asList(result.get("mbeans"))).isNotEmpty();
        assertThat(result.get("hiddenByPolicy")).isEqualTo(0);

        Map<String, Object> memory = asList(result.get("mbeans"))
                .stream()
                        .map(MBeanToolsTest::asMap)
                        .filter(m -> MEMORY.equals(m.get("name")))
                        .findFirst()
                        .orElseThrow();
        assertThat(asList(memory.get("attributes"))).contains("HeapMemoryUsage", "NonHeapMemoryUsage");
    }

    @Test
    void includeAttributesFalseSkipsThePerBeanRoundTrip() {
        ToolOutcome outcome = call(
                adapter(policy().build()),
                "jvm.mbeans",
                Map.of("pattern", "java.lang:*", "include_attributes", false));

        List<Object> beans = asList(structured(outcome).get("mbeans"));
        assertThat(beans).isNotEmpty();
        assertThat(asMap(beans.get(0))).containsOnlyKeys("name");
    }

    /**
     * The distinction the tool exists to draw: beans that are there and beans this caller may
     * see. A model told "none" concludes the JVM has no memory beans; told "hidden", it knows to
     * ask a human for access rather than to look somewhere else.
     */
    @Test
    void beansOutsideTheAllowlistAreCountedRatherThanImplicitlyAbsent() {
        JvmAdapter restricted = adapter(PolicyProfile.builder("self")
                .allowRead("java.lang:type=Runtime")
                .maxRows(200)
                .build());

        ToolOutcome outcome = call(restricted, "jvm.mbeans", Map.of("pattern", "java.lang:*"));

        Map<String, Object> result = structured(outcome);
        assertThat(asList(result.get("mbeans"))).hasSize(1);
        assertThat((Integer) result.get("hiddenByPolicy")).isPositive();
        assertThat(outcome.summary()).contains("java.lang:type=Runtime");
    }

    @Test
    void aWholeQueryHiddenByPolicyExplainsWhatIsPermitted() {
        JvmAdapter restricted = adapter(PolicyProfile.builder("self")
                .allowRead("com.example:*")
                .build());

        ToolOutcome outcome = call(restricted, "jvm.mbeans", Map.of("pattern", "java.lang:*"));

        assertThat(outcome.error()).isFalse();
        assertThat(asList(structured(outcome).get("mbeans"))).isEmpty();
        assertThat(outcome.summary())
                .contains("none are on this server's allowlist")
                .contains("com.example:*");
    }

    @Test
    void resultCapTruncatesAndSaysSo() {
        ToolOutcome outcome = call(
                adapter(policy().maxRows(2).build()), "jvm.mbeans", Map.of("pattern", "java.lang:*"));

        Map<String, Object> result = structured(outcome);
        assertThat(asList(result.get("mbeans"))).hasSize(2);
        assertThat(result.get("truncated")).isEqualTo(true);
        assertThat(outcome.summary()).contains("Narrow the pattern");
    }

    @Test
    void malformedPatternIsExplained() {
        ToolOutcome outcome = call(adapter(policy().build()), "jvm.mbeans", Map.of("pattern", "not a name"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("not a valid JMX ObjectName pattern").contains("java.lang:*");
    }

    @Test
    void readsACompositeAttributeAsANestedObject() {
        ToolOutcome outcome = call(
                adapter(policy().build()),
                "jvm.attribute",
                Map.of("mbean", MEMORY, "attributes", List.of("HeapMemoryUsage")));

        assertThat(outcome.error()).isFalse();
        Map<String, Object> values = asMap(structured(outcome).get("values"));
        Map<String, Object> heap = asMap(values.get("HeapMemoryUsage"));
        assertThat(heap).containsKeys("init", "used", "committed", "max");
        assertThat((Long) heap.get("used")).isPositive();
    }

    @Test
    void omittingAttributesReadsAllOfThem() {
        ToolOutcome outcome = call(adapter(policy().build()), "jvm.attribute", Map.of("mbean", MEMORY));

        Map<String, Object> values = asMap(structured(outcome).get("values"));
        assertThat(values).containsKeys("HeapMemoryUsage", "NonHeapMemoryUsage", "ObjectPendingFinalizationCount");
    }

    /**
     * A tabular attribute of {@code {key, value}} rows is flattened into an object, because the
     * literal rendering is 400 two-element records and no model reads that to answer one question.
     */
    @Test
    void systemPropertiesAreFlattenedIntoAnIndexableObject() {
        ToolOutcome outcome = call(
                adapter(policy().build()),
                "jvm.attribute",
                Map.of("mbean", RUNTIME, "attributes", List.of("SystemProperties")));

        Map<String, Object> values = asMap(structured(outcome).get("values"));
        Map<String, Object> properties = asMap(values.get("SystemProperties"));
        assertThat(properties.get("java.version")).isEqualTo(System.getProperty("java.version"));
    }

    /**
     * The redaction case that actually happens. Credentials on a JVM are not an attribute called
     * Password; they are a key inside SystemProperties, so a redactor that only matched the outer
     * attribute name would be a guardrail for a case that does not occur.
     */
    @Test
    void redactionReachesInsideATabularValue() {
        System.setProperty("bridge.test.datasource.password", "hunter2");
        try {
            ToolOutcome outcome = call(
                    adapter(policy().redact("*password*").build()),
                    "jvm.attribute",
                    Map.of("mbean", RUNTIME, "attributes", List.of("SystemProperties")));

            Map<String, Object> values = asMap(structured(outcome).get("values"));
            Map<String, Object> properties = asMap(values.get("SystemProperties"));
            assertThat(properties.get("bridge.test.datasource.password")).isEqualTo(Redactor.MARKER);
            assertThat(properties.get("java.version")).isEqualTo(System.getProperty("java.version"));
            assertThat(structured(outcome).get("redacted")).isEqualTo(true);
        } finally {
            System.clearProperty("bridge.test.datasource.password");
        }
    }

    @Test
    void redactionAlsoAppliesToAWholeAttribute() {
        ToolOutcome outcome = call(
                adapter(policy().redact("java.lang:type=Runtime.InputArguments").build()),
                "jvm.attribute",
                Map.of("mbean", RUNTIME, "attributes", List.of("InputArguments", "Name")));

        Map<String, Object> values = asMap(structured(outcome).get("values"));
        assertThat(values.get("InputArguments")).isEqualTo(Redactor.MARKER);
        assertThat(values.get("Name")).isNotEqualTo(Redactor.MARKER);
    }

    /** A partial list that does not say it is partial is a wrong answer, not a short one. */
    @Test
    void aValueCutByTheElementCapIsMarkedTruncated() {
        ToolOutcome outcome = call(
                adapter(policy().maxRows(3).build()),
                "jvm.attribute",
                Map.of("mbean", RUNTIME, "attributes", List.of("SystemProperties")));

        Map<String, Object> result = structured(outcome);
        assertThat(result.get("truncated")).isEqualTo(true);
        Map<String, Object> properties = asMap(asMap(result.get("values")).get("SystemProperties"));
        assertThat(properties).hasSize(4).containsKey(JmxValues.TRUNCATED);
        assertThat(outcome.summary()).contains("cut at this server's size caps");
    }

    @Test
    void anMBeanOutsideTheAllowlistIsDeniedWithTheAlternatives() {
        JvmAdapter restricted = adapter(PolicyProfile.builder("self")
                .allowRead("java.lang:type=Runtime")
                .build());

        ToolOutcome outcome = call(restricted, "jvm.attribute", Map.of("mbean", MEMORY));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("java.lang:type=runtime");
    }

    @Test
    void aPatternIsRefusedWithTheToolThatTakesOne() {
        ToolOutcome outcome = call(adapter(policy().build()), "jvm.attribute", Map.of("mbean", "java.lang:*"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("is a pattern").contains("jvm.mbeans");
    }

    @Test
    void anAllowlistedButUnregisteredBeanIsAnActionableFailure() {
        ToolOutcome outcome = call(
                adapter(policy().allowRead("java.lang:type=NoSuchBean").build()),
                "jvm.attribute",
                Map.of("mbean", "java.lang:type=NoSuchBean"));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("is not").contains("registered").contains("jvm.mbeans");
    }

    @Test
    void unknownAttributeNamesAreReportedRatherThanIgnored() {
        ToolOutcome outcome = call(
                adapter(policy().build()),
                "jvm.attribute",
                Map.of("mbean", MEMORY, "attributes", List.of("HeapMemoryUsage", "Nonsense")));

        Map<String, Object> result = structured(outcome);
        assertThat(asList(result.get("unknown"))).containsExactly("Nonsense");
        assertThat(asMap(result.get("values"))).containsKey("HeapMemoryUsage");
    }

    @Test
    void askingOnlyForAttributesThatDoNotExistListsTheOnesThatDo() {
        ToolOutcome outcome = call(
                adapter(policy().build()),
                "jvm.attribute",
                Map.of("mbean", MEMORY, "attributes", List.of("Nonsense")));

        assertThat(outcome.error()).isTrue();
        assertThat(outcome.summary()).contains("HeapMemoryUsage");
    }

    /** The allowlist is matched on the canonical name, so property order cannot evade it. */
    @Test
    void objectNamePropertyOrderDoesNotChangeTheDecision() throws Exception {
        ObjectName pool = ManagementFactory.getMemoryPoolMXBeans().isEmpty()
                ? null
                : new ObjectName(ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE + ",name="
                        + ManagementFactory.getMemoryPoolMXBeans().get(0).getName());
        assertThat(pool).isNotNull();

        PolicyProfile profile = policy().build();
        assertThat(MBeanTools.readable(profile, pool)).isTrue();
        assertThat(MBeanTools.readable(
                        PolicyProfile.builder("self").allowRead("java.lang:type=memory").build(), pool))
                .isFalse();
    }

    @Test
    void dryRunModeReturnsThePlanWithoutTouchingTheMBeanServer() {
        ToolOutcome outcome =
                call(adapter(policy().mode(AccessMode.DRY_RUN).build()), "jvm.attribute", Map.of("mbean", MEMORY));

        assertThat(outcome.error()).isFalse();
        assertThat(structured(outcome)).containsEntry("dryRun", true);
        assertThat(outcome.summary()).contains("Nothing was executed");
    }

    @Test
    void everyCallIsAudited() {
        AuditSink.RecordingAuditSink audit = AuditSink.recording();
        adapter = new JvmAdapter(JvmTargetHandle.embedded("self"), policy().build(), audit);

        call(adapter, "jvm.mbeans", Map.of("pattern", "java.lang:*"));
        call(adapter, "jvm.attribute", Map.of("mbean", "java.nio:type=BufferPool,name=direct"));

        assertThat(audit.records()).extracting("tool").containsExactly("jvm.mbeans", "jvm.attribute");
        assertThat(audit.records().get(1).allowed()).isFalse();
    }
}
