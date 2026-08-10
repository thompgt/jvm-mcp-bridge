package io.github.thompgt.jvmmcp.server;

import io.github.thompgt.jvmmcp.policy.AccessMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

/**
 * Binds {@code bridge.yaml}.
 *
 * <p>Deliberately plain: no validation logic here beyond what Boot does for types. The real
 * checks live in {@code PolicyProfile.Builder}, so that a configuration mistake is caught by
 * the same code whether it arrives from YAML, a test, or a future admin API.
 */
@ConfigurationProperties(prefix = "bridge")
public class BridgeProperties {

    /** Global default; a datasource may not exceed it, only narrow it. */
    private AccessMode mode = AccessMode.READ_ONLY;

    private Transport transport = Transport.STDIO;

    private Audit audit = new Audit();

    private Http http = new Http();

    private List<Datasource> datasources = new ArrayList<>();

    private List<Broker> brokers = new ArrayList<>();

    private List<Jvm> jvms = new ArrayList<>();

    public enum Transport {
        /** Launched as a subprocess by an MCP client. */
        STDIO,
        /** Streamable HTTP on /mcp, for a shared deployment. Phase 2. */
        HTTP
    }

    public static class Audit {
        /** When set, records go to this file as JSON lines. Otherwise to the application log. */
        private String file;

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }
    }

    /** Settings that apply only to {@code transport: http}. */
    public static class Http {
        /** Path the MCP endpoint is served on. */
        private String endpoint = "/mcp";

        /**
         * Origins permitted to reach the endpoint from a browser. Empty means none — a
         * deliberate default, because an MCP server bound to localhost with no Origin check is
         * reachable by any page the user visits (DNS rebinding). Set this only if a browser
         * client genuinely needs it.
         */
        private List<String> allowedOrigins = new ArrayList<>();

        /** Host headers permitted, for the same reason. Empty means no Host restriction. */
        private List<String> allowedHosts = new ArrayList<>();

        /**
         * How often to send a keep-alive on an open SSE stream. Proxies and load balancers cut
         * idle connections; a long-running tool call looks idle to them.
         */
        private Duration keepAliveInterval = Duration.ofSeconds(30);

        private Auth auth = new Auth();

        public Auth getAuth() {
            return auth;
        }

        public void setAuth(Auth auth) {
            this.auth = auth;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public List<String> getAllowedOrigins() {
            return allowedOrigins;
        }

        public void setAllowedOrigins(List<String> allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }

        public List<String> getAllowedHosts() {
            return allowedHosts;
        }

        public void setAllowedHosts(List<String> allowedHosts) {
            this.allowedHosts = allowedHosts;
        }

        public Duration getKeepAliveInterval() {
            return keepAliveInterval;
        }

        public void setKeepAliveInterval(Duration keepAliveInterval) {
            this.keepAliveInterval = keepAliveInterval;
        }
    }

    /** How callers on the HTTP transport prove who they are. */
    public static class Auth {

        public enum Mode {
            /**
             * No authentication. Every caller is anonymous and gets the default profile.
             * Legitimate only when something in front of the bridge already authenticates, and
             * refused unless {@code bridge.http.auth.i-understand-this-is-unauthenticated} is
             * also set — an MCP endpoint open to the network is an open database connection.
             */
            NONE,
            /** Static keys mapped to principals. For internal networks. */
            API_KEY,
            /** JWT bearer tokens validated against an issuer. Phase 2.3. */
            OAUTH2
        }

        private Mode mode = Mode.API_KEY;

        /** Required to actually run with {@link Mode#NONE}. Named to be hard to set by accident. */
        private boolean iUnderstandThisIsUnauthenticated;

        /** Header carrying the credential. Bearer tokens use {@code Authorization}. */
        private String header = "Authorization";

        private List<ApiKey> keys = new ArrayList<>();

        private OAuth2 oauth2 = new OAuth2();

        public OAuth2 getOauth2() {
            return oauth2;
        }

        public void setOauth2(OAuth2 oauth2) {
            this.oauth2 = oauth2;
        }

        public Mode getMode() {
            return mode;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public boolean isIUnderstandThisIsUnauthenticated() {
            return iUnderstandThisIsUnauthenticated;
        }

        public void setIUnderstandThisIsUnauthenticated(boolean value) {
            this.iUnderstandThisIsUnauthenticated = value;
        }

        public String getHeader() {
            return header;
        }

        public void setHeader(String header) {
            this.header = header;
        }

        public List<ApiKey> getKeys() {
            return keys;
        }

        public void setKeys(List<ApiKey> keys) {
            this.keys = keys;
        }
    }

    /** JWT bearer validation for deployments that already have an identity provider. */
    public static class OAuth2 {
        /** Issuer to fetch signing keys and metadata from. Its {@code iss} must match exactly. */
        private String issuerUri;

        /**
         * The resource indicator (RFC 8707) identifying <em>this</em> bridge, matched against the
         * token's {@code aud}. Required, and the single most important setting here: without it
         * a token minted for any other service the same issuer serves would be accepted, which
         * is the confused-deputy problem the 2025-06-18 security revision added it to close.
         */
        private String audience;

        /** Claim to take the audited identity from. */
        private String principalClaim = "sub";

        /** Optional claim naming the policy profile to apply. Absent means the backend default. */
        private String profileClaim;

        /** Optional scope the token must carry, checked against {@code scope} or {@code scp}. */
        private String requiredScope;

        public String getIssuerUri() {
            return issuerUri;
        }

        public void setIssuerUri(String issuerUri) {
            this.issuerUri = issuerUri;
        }

        public String getAudience() {
            return audience;
        }

        public void setAudience(String audience) {
            this.audience = audience;
        }

        public String getPrincipalClaim() {
            return principalClaim;
        }

        public void setPrincipalClaim(String principalClaim) {
            this.principalClaim = principalClaim;
        }

        public String getProfileClaim() {
            return profileClaim;
        }

        public void setProfileClaim(String profileClaim) {
            this.profileClaim = profileClaim;
        }

        public String getRequiredScope() {
            return requiredScope;
        }

        public void setRequiredScope(String requiredScope) {
            this.requiredScope = requiredScope;
        }
    }

    public static class ApiKey {
        /** The secret. Use a placeholder like {@code ${ANALYST_KEY}} — do not commit a literal. */
        private String key;

        /** Identity written to the audit log for calls made with this key. */
        private String principal;

        /** Policy profile to apply; omitted means the backend default. */
        private String profile;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getPrincipal() {
            return principal;
        }

        public void setPrincipal(String principal) {
            this.principal = principal;
        }

        public String getProfile() {
            return profile;
        }

        public void setProfile(String profile) {
            this.profile = profile;
        }
    }

    public static class Datasource {
        private String name;
        private String url;
        private String username;
        private String password;
        private Policy policy = new Policy();
        private Map<String, ProfileOverride> profiles = new LinkedHashMap<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Policy getPolicy() {
            return policy;
        }

        public void setPolicy(Policy policy) {
            this.policy = policy;
        }

        /** Named profiles, keyed by the name an API key or a token claim refers to. */
        public Map<String, ProfileOverride> getProfiles() {
            return profiles;
        }

        public void setProfiles(Map<String, ProfileOverride> profiles) {
            this.profiles = profiles;
        }
    }

    /**
     * A named profile, expressed as the differences from the datasource's own policy block.
     *
     * <p>Every field is nullable and unset means inherit. Writing it as a delta rather than a
     * whole policy is what makes the common case — "the same, but only these two tables" —
     * three lines of YAML, and it removes a class of mistake: an operator who copies the whole
     * block to change one table has to keep the other five values in step with the default
     * forever, and the day they don't, the profile silently stops narrowing what they meant.
     *
     * <p>Widening is still rejected at startup by {@code PolicyProfiles}; this only makes the
     * usual case impossible to get wrong rather than merely caught.
     */
    public static class ProfileOverride {
        private AccessMode mode;
        private List<String> allowTables;
        private List<String> allowWriteTables;
        private Integer maxRows;
        private DataSize maxResultBytes;
        private Duration statementTimeout;

        /**
         * Additional redaction, always added to the datasource's own. A profile may not un-redact
         * something the backend redacts, so there is no way to express removal here.
         */
        private List<String> redactColumns = new ArrayList<>();

        public AccessMode getMode() {
            return mode;
        }

        public void setMode(AccessMode mode) {
            this.mode = mode;
        }

        public List<String> getAllowTables() {
            return allowTables;
        }

        public void setAllowTables(List<String> allowTables) {
            this.allowTables = allowTables;
        }

        public List<String> getAllowWriteTables() {
            return allowWriteTables;
        }

        public void setAllowWriteTables(List<String> allowWriteTables) {
            this.allowWriteTables = allowWriteTables;
        }

        public Integer getMaxRows() {
            return maxRows;
        }

        public void setMaxRows(Integer maxRows) {
            this.maxRows = maxRows;
        }

        public DataSize getMaxResultBytes() {
            return maxResultBytes;
        }

        public void setMaxResultBytes(DataSize maxResultBytes) {
            this.maxResultBytes = maxResultBytes;
        }

        public Duration getStatementTimeout() {
            return statementTimeout;
        }

        public void setStatementTimeout(Duration statementTimeout) {
            this.statementTimeout = statementTimeout;
        }

        public List<String> getRedactColumns() {
            return redactColumns;
        }

        public void setRedactColumns(List<String> redactColumns) {
            this.redactColumns = redactColumns;
        }
    }

    public static class Broker {
        private String name;

        /** {@code host:port} list, exactly as a Kafka client takes it. */
        private String bootstrapServers;

        private BrokerPolicy policy = new BrokerPolicy();

        private Map<String, ProfileOverride> profiles = new LinkedHashMap<>();

        /**
         * Raw Kafka client settings, passed through untouched: {@code security.protocol},
         * {@code sasl.jaas.config}, truststore paths. Every real cluster needs some of these and
         * enumerating them here would be a losing race with the Kafka release cycle.
         */
        private Map<String, String> client = new LinkedHashMap<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public BrokerPolicy getPolicy() {
            return policy;
        }

        public void setPolicy(BrokerPolicy policy) {
            this.policy = policy;
        }

        public Map<String, ProfileOverride> getProfiles() {
            return profiles;
        }

        public void setProfiles(Map<String, ProfileOverride> profiles) {
            this.profiles = profiles;
        }

        public Map<String, String> getClient() {
            return client;
        }

        public void setClient(Map<String, String> client) {
            this.client = client;
        }
    }

    /**
     * The same shape as {@link Policy} in a different vocabulary: topics instead of tables,
     * messages instead of rows.
     *
     * <p>Kept as its own type rather than reusing {@code Policy} with generic names. Both bind
     * to the same {@code PolicyProfile}, but the YAML an operator writes should say
     * {@code allow-topics}, and a shared type would force one backend's words onto the other.
     */
    public static class BrokerPolicy {
        /** Wildcards are expected here — {@code orders.*} is how topic families are named. */
        private List<String> allowTopics = new ArrayList<>();

        /** Enumerated only; {@code *} is rejected. Requires {@code mode: read-write} as well. */
        private List<String> allowWriteTopics = new ArrayList<>();

        private int maxMessages = 25;
        private DataSize maxResultBytes = DataSize.ofKilobytes(512);
        private Duration requestTimeout = Duration.ofSeconds(10);
        private List<String> redactHeaders = new ArrayList<>();

        public List<String> getAllowTopics() {
            return allowTopics;
        }

        public void setAllowTopics(List<String> allowTopics) {
            this.allowTopics = allowTopics;
        }

        public List<String> getAllowWriteTopics() {
            return allowWriteTopics;
        }

        public void setAllowWriteTopics(List<String> allowWriteTopics) {
            this.allowWriteTopics = allowWriteTopics;
        }

        public int getMaxMessages() {
            return maxMessages;
        }

        public void setMaxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
        }

        public DataSize getMaxResultBytes() {
            return maxResultBytes;
        }

        public void setMaxResultBytes(DataSize maxResultBytes) {
            this.maxResultBytes = maxResultBytes;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public List<String> getRedactHeaders() {
            return redactHeaders;
        }

        public void setRedactHeaders(List<String> redactHeaders) {
            this.redactHeaders = redactHeaders;
        }
    }

    public static class Jvm {
        private String name;

        /**
         * A {@code service:jmx:…} URL for another process, or omitted to describe the JVM this
         * bridge runs in.
         *
         * <p>Omitting it is the honest default for a bridge embedded in the application it
         * reports on, and close to useless otherwise: the local MBeanServer describes this
         * process, so a standalone bridge with no URL answers every question about itself and
         * none about the service anyone is asking after.
         */
        private String jmxUrl;

        /** JMX credentials, when the target's connector authenticates. Use a placeholder. */
        private String username;

        private String password;

        /**
         * The application's Actuator root, e.g. {@code http://localhost:8080/actuator}. Omitted,
         * no {@code jvm.actuator} tool is registered at all — there is nothing for it to reach.
         */
        private String actuatorBaseUrl;

        /** HTTP basic credentials for a secured Actuator. */
        private String actuatorUsername;

        private String actuatorPassword;

        /** Bearer token, used when no {@code actuator-username} is given. */
        private String actuatorToken;

        private JvmPolicy policy = new JvmPolicy();

        private Map<String, ProfileOverride> profiles = new LinkedHashMap<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getJmxUrl() {
            return jmxUrl;
        }

        public void setJmxUrl(String jmxUrl) {
            this.jmxUrl = jmxUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getActuatorBaseUrl() {
            return actuatorBaseUrl;
        }

        public void setActuatorBaseUrl(String actuatorBaseUrl) {
            this.actuatorBaseUrl = actuatorBaseUrl;
        }

        public String getActuatorUsername() {
            return actuatorUsername;
        }

        public void setActuatorUsername(String actuatorUsername) {
            this.actuatorUsername = actuatorUsername;
        }

        public String getActuatorPassword() {
            return actuatorPassword;
        }

        public void setActuatorPassword(String actuatorPassword) {
            this.actuatorPassword = actuatorPassword;
        }

        public String getActuatorToken() {
            return actuatorToken;
        }

        public void setActuatorToken(String actuatorToken) {
            this.actuatorToken = actuatorToken;
        }

        public JvmPolicy getPolicy() {
            return policy;
        }

        public void setPolicy(JvmPolicy policy) {
            this.policy = policy;
        }

        public Map<String, ProfileOverride> getProfiles() {
            return profiles;
        }

        public void setProfiles(Map<String, ProfileOverride> profiles) {
            this.profiles = profiles;
        }
    }

    /** {@link Policy} in the vocabulary of a JVM: MBeans instead of tables, attributes instead of columns. */
    public static class JvmPolicy {
        /** ObjectName patterns. {@code java.lang:*} is the platform's own beans. */
        private List<String> allowMbeans = new ArrayList<>();

        /**
         * Actuator endpoints, named one at a time: {@code health}, {@code env}, {@code metrics}.
         *
         * <p>Enumerated rather than wildcarded because Actuator endpoints are nothing like each
         * other in sensitivity. {@code health} is a status word; {@code env} is the application's
         * whole resolved configuration; {@code heapdump} is every object in memory, including
         * every string that has held a password. A group grant would mean the operator who wanted
         * the first had given away the third.
         *
         * <p>Empty by default, so an Actuator that is configured is still not readable until
         * someone says which parts of it are.
         */
        private List<String> allowActuator = new ArrayList<>();

        /**
         * Caps both how many MBeans a listing returns and how many elements of a single
         * attribute value are rendered — a JVM with 5000 live threads has an {@code AllThreadIds}
         * that is longer than any useful answer.
         */
        private int maxResults = 100;

        private DataSize maxResultBytes = DataSize.ofKilobytes(256);

        private Duration requestTimeout = Duration.ofSeconds(10);

        /**
         * Patterns matched against {@code ObjectName.attribute} <em>and</em> against keys nested
         * inside a composite or tabular value.
         *
         * <p>Non-empty by default, which no other backend's policy is, and the asymmetry is
         * deliberate. A database exposes credentials only if someone stored them in a table; a
         * JVM exposes them structurally — {@code SystemProperties} and {@code InputArguments}
         * carry the datasource password of essentially every Spring Boot application ever
         * started with one on the command line. An operator who configures a JVM target and
         * thinks no further about redaction should not thereby hand those to a model. Replace
         * this list to change it; the entries are patterns, not a hardcoded rule.
         */
        private List<String> redactAttributes = new ArrayList<>(
                List.of("*password*", "*secret*", "*credential*", "*passwd*", "*apikey*", "*api-key*", "*.token"));

        public List<String> getAllowMbeans() {
            return allowMbeans;
        }

        public void setAllowMbeans(List<String> allowMbeans) {
            this.allowMbeans = allowMbeans;
        }

        public List<String> getAllowActuator() {
            return allowActuator;
        }

        public void setAllowActuator(List<String> allowActuator) {
            this.allowActuator = allowActuator;
        }

        public int getMaxResults() {
            return maxResults;
        }

        public void setMaxResults(int maxResults) {
            this.maxResults = maxResults;
        }

        public DataSize getMaxResultBytes() {
            return maxResultBytes;
        }

        public void setMaxResultBytes(DataSize maxResultBytes) {
            this.maxResultBytes = maxResultBytes;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public List<String> getRedactAttributes() {
            return redactAttributes;
        }

        public void setRedactAttributes(List<String> redactAttributes) {
            this.redactAttributes = redactAttributes;
        }
    }

    public static class Policy {
        private List<String> allowTables = new ArrayList<>();
        private List<String> allowWriteTables = new ArrayList<>();
        private int maxRows = 100;
        private DataSize maxResultBytes = DataSize.ofMegabytes(1);
        private Duration statementTimeout = Duration.ofSeconds(5);
        private List<String> redactColumns = new ArrayList<>();

        public List<String> getAllowTables() {
            return allowTables;
        }

        public void setAllowTables(List<String> allowTables) {
            this.allowTables = allowTables;
        }

        public List<String> getAllowWriteTables() {
            return allowWriteTables;
        }

        public void setAllowWriteTables(List<String> allowWriteTables) {
            this.allowWriteTables = allowWriteTables;
        }

        public int getMaxRows() {
            return maxRows;
        }

        public void setMaxRows(int maxRows) {
            this.maxRows = maxRows;
        }

        public DataSize getMaxResultBytes() {
            return maxResultBytes;
        }

        public void setMaxResultBytes(DataSize maxResultBytes) {
            this.maxResultBytes = maxResultBytes;
        }

        public Duration getStatementTimeout() {
            return statementTimeout;
        }

        public void setStatementTimeout(Duration statementTimeout) {
            this.statementTimeout = statementTimeout;
        }

        public List<String> getRedactColumns() {
            return redactColumns;
        }

        public void setRedactColumns(List<String> redactColumns) {
            this.redactColumns = redactColumns;
        }
    }

    public AccessMode getMode() {
        return mode;
    }

    public void setMode(AccessMode mode) {
        this.mode = mode;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public Audit getAudit() {
        return audit;
    }

    public void setAudit(Audit audit) {
        this.audit = audit;
    }

    public Http getHttp() {
        return http;
    }

    public void setHttp(Http http) {
        this.http = http;
    }

    public List<Datasource> getDatasources() {
        return datasources;
    }

    public void setDatasources(List<Datasource> datasources) {
        this.datasources = datasources;
    }

    public List<Broker> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<Broker> brokers) {
        this.brokers = brokers;
    }

    public List<Jvm> getJvms() {
        return jvms;
    }

    public void setJvms(List<Jvm> jvms) {
        this.jvms = jvms;
    }
}
