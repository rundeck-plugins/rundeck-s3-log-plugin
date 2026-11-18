package org.rundeck.plugins;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Request-level tests for the S3 client built by {@link S3LogFileStoragePlugin}.
 *
 * <p>A local HTTP/HTTPS server stands in for an S3-compatible service and records the raw
 * requests the plugin sends, so the assertions cover what goes over the wire (headers, path,
 * host and decoded body) instead of internal builder calls.</p>
 */
public class S3LogFileStoragePluginClientTest {

    private static final String BUCKET = "rdbucket";
    private static final String KEY = "logs/testexecid.rdlog";
    private static final String PASSWORD = "changeit";

    /** Payload larger than 128 KiB so the SDK has to stream it in more than one chunk. */
    private static final byte[] PAYLOAD = buildPayload(200 * 1024);

    @ClassRule
    public static final TemporaryFolder TMP = new TemporaryFolder();

    private static final String[] TRUST_STORE_PROPERTIES = {
            "javax.net.ssl.trustStore", "javax.net.ssl.trustStorePassword", "javax.net.ssl.trustStoreType"};

    private static File keyStoreFile;
    private static final Map<String, String> previousTrustStoreProperties = new HashMap<>();

    private static final String ENDPOINT_URL_S3_PROPERTY = "aws.endpointUrlS3";

    private HttpServer server;
    private String previousEndpointUrlS3;
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

    /**
     * Creates a self-signed certificate for localhost and makes the JVM trust it so the SDK's
     * default HTTP client can talk to the local HTTPS server.
     */
    @BeforeClass
    public static void createCertificate() throws Exception {
        keyStoreFile = new File(TMP.getRoot(), "server.p12");
        String keytool = new File(System.getProperty("java.home"), "bin/keytool").getAbsolutePath();
        Process process = new ProcessBuilder(
                keytool, "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "2", "-dname", "CN=localhost", "-storetype", "PKCS12",
                "-keystore", keyStoreFile.getAbsolutePath(), "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-ext", "SAN=dns:localhost,dns:" + BUCKET + ".localhost,ip:127.0.0.1")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        Assert.assertEquals("keytool failed: " + output, 0, process.waitFor());
        for (String name : TRUST_STORE_PROPERTIES) {
            previousTrustStoreProperties.put(name, System.getProperty(name));
        }
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    }

    /**
     * Restores the trust store system properties changed by {@link #createCertificate()}.
     */
    @AfterClass
    public static void restoreTrustStoreProperties() {
        previousTrustStoreProperties.forEach((name, value) -> {
            if (value == null) {
                System.clearProperty(name);
            } else {
                System.setProperty(name, value);
            }
        });
        previousTrustStoreProperties.clear();
    }

    /**
     * Captures the SDK endpoint system property so each test can restore it afterwards.
     */
    @Before
    public void captureEndpointProperty() {
        previousEndpointUrlS3 = System.getProperty(ENDPOINT_URL_S3_PROPERTY);
    }

    /**
     * Stops the fake S3 server and restores the SDK endpoint system property to its previous
     * value, clearing it only when it was not set before the test.
     */
    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
        if (previousEndpointUrlS3 == null) {
            System.clearProperty(ENDPOINT_URL_S3_PROPERTY);
        } else {
            System.setProperty(ENDPOINT_URL_S3_PROPERTY, previousEndpointUrlS3);
        }
    }

    @Test
    public void customHttpEndpointSendsNoChecksumAndPreservesBody() throws Exception {
        RecordedRequest request = storeThroughPlugin(false, true, true);

        assertNoChecksum(request);
        assertPathStyle(request);
        // Plain HTTP keeps the SDK's signed aws-chunked streaming; the full body must survive it.
        Assert.assertEquals("aws-chunked", request.header("content-encoding"));
        Assert.assertEquals(String.valueOf(PAYLOAD.length), request.header("x-amz-decoded-content-length"));
        Assert.assertArrayEquals(PAYLOAD, request.decodedBody());
    }

    @Test
    public void customHttpsEndpointSendsNoChecksumAndPreservesBody() throws Exception {
        RecordedRequest request = storeThroughPlugin(true, true, true);

        assertNoChecksum(request);
        assertPathStyle(request);
        Assert.assertArrayEquals(PAYLOAD, request.decodedBody());
    }

    @Test
    public void customHttpEndpointWithVirtualHostStyleSendsNoChecksum() throws Exception {
        RecordedRequest request = storeThroughPlugin(false, false, true);

        assertNoChecksum(request);
        assertVirtualHostStyle(request);
        Assert.assertArrayEquals(PAYLOAD, request.decodedBody());
    }

    @Test
    public void customHttpsEndpointWithVirtualHostStyleSendsNoChecksum() throws Exception {
        RecordedRequest request = storeThroughPlugin(true, false, true);

        assertNoChecksum(request);
        assertVirtualHostStyle(request);
        Assert.assertArrayEquals(PAYLOAD, request.decodedBody());
    }

    /**
     * Without a configured endpoint the plugin must keep the SDK defaults used for AWS S3,
     * which add a flexible checksum to uploads. The SDK endpoint system property routes the
     * request to the local server without touching the plugin configuration.
     */
    @Test
    public void noEndpointKeepsSdkDefaultChecksum() throws Exception {
        RecordedRequest request = storeThroughPlugin(true, true, false);

        Assert.assertTrue("expected SDK default checksum, headers: " + request.headers,
                request.header("x-amz-sdk-checksum-algorithm") != null
                        || request.header("x-amz-trailer") != null);
        Assert.assertArrayEquals(PAYLOAD, request.decodedBody());
    }

    private RecordedRequest storeThroughPlugin(boolean https, boolean pathStyle, boolean pluginEndpoint)
            throws Exception {
        String endpoint = startServer(https);
        S3LogFileStoragePlugin plugin = new S3LogFileStoragePlugin();
        plugin.setAWSAccessKeyId("test-access-key");
        plugin.setAWSSecretKey("test-secret-key");
        plugin.setBucket(BUCKET);
        plugin.setPath("logs/${job.execid}");
        plugin.setRegion("us-east-1");
        plugin.setPathStyle(pathStyle);
        if (pluginEndpoint) {
            plugin.setEndpoint(endpoint);
        } else {
            System.setProperty(ENDPOINT_URL_S3_PROPERTY, endpoint);
        }
        plugin.initialize(context());
        try {
            Assert.assertTrue(plugin.store("rdlog", new ByteArrayInputStream(PAYLOAD), PAYLOAD.length, new Date()));
        } finally {
            plugin.getS3Client().close();
        }
        Assert.assertEquals(1, requests.size());
        RecordedRequest request = requests.get(0);
        Assert.assertEquals("PUT", request.method);
        return request;
    }

    private String startServer(boolean https) throws Exception {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 0);
        if (https) {
            HttpsServer httpsServer = HttpsServer.create(address, 0);
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(serverSslContext()));
            server = httpsServer;
        } else {
            server = HttpServer.create(address, 0);
        }
        server.createContext("/", this::record);
        server.start();
        return (https ? "https" : "http") + "://localhost:" + server.getAddress().getPort();
    }

    private static SSLContext serverSslContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(keyStoreFile)) {
            keyStore.load(in, PASSWORD.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    private void record(HttpExchange exchange) throws IOException {
        Map<String, String> headers = new HashMap<>();
        exchange.getRequestHeaders().forEach((name, values) ->
                headers.put(name.toLowerCase(Locale.ROOT), String.join(",", values)));
        byte[] body = exchange.getRequestBody().readAllBytes();
        requests.add(new RecordedRequest(exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(),
                headers, body));
        exchange.getResponseHeaders().add("ETag", "\"etag\"");
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private static void assertNoChecksum(RecordedRequest request) {
        List<String> checksumHeaders = new ArrayList<>();
        request.headers.forEach((name, value) -> {
            if (name.startsWith("x-amz-checksum-") || name.equals("x-amz-sdk-checksum-algorithm")
                    || name.equals("x-amz-trailer")) {
                checksumHeaders.add(name + ": " + value);
            }
        });
        Assert.assertEquals("unexpected checksum headers", new ArrayList<String>(), checksumHeaders);
        String sha = request.header("x-amz-content-sha256");
        Assert.assertFalse("unexpected trailer payload mode: " + sha, sha != null && sha.endsWith("-TRAILER"));
        Assert.assertTrue("unexpected checksum trailer in body",
                request.trailers.isEmpty());
    }

    private static void assertPathStyle(RecordedRequest request) {
        Assert.assertEquals("/" + BUCKET + "/" + KEY, request.path);
        Assert.assertTrue(request.header("host"), request.header("host").startsWith("localhost:"));
    }

    private static void assertVirtualHostStyle(RecordedRequest request) {
        Assert.assertEquals("/" + KEY, request.path);
        Assert.assertTrue(request.header("host"), request.header("host").startsWith(BUCKET + ".localhost:"));
    }

    private static HashMap<String, Object> context() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("execid", "testexecid");
        map.put(S3LogFileStoragePlugin.META_ID_FOR_LOGSTORE, "testexecid");
        map.put("project", "testproject");
        return map;
    }

    private static byte[] buildPayload(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ((i * 31 + i / 7) & 0xff);
        }
        return data;
    }

    /** One request as received by the fake S3 service. */
    private static final class RecordedRequest {
        final String method;
        final String path;
        final Map<String, String> headers;
        final byte[] rawBody;
        final List<String> trailers = new ArrayList<>();
        private byte[] decoded;

        RecordedRequest(String method, String path, Map<String, String> headers, byte[] rawBody) {
            this.method = method;
            this.path = path;
            this.headers = headers;
            this.rawBody = rawBody;
            this.decoded = decode();
        }

        String header(String name) {
            return headers.get(name);
        }

        byte[] decodedBody() {
            return decoded;
        }

        /**
         * Decodes an {@code aws-chunked} body the way an S3-compatible server would, collecting
         * any trailing headers. Plain bodies are returned unchanged.
         */
        private byte[] decode() {
            String encoding = header("content-encoding");
            if (encoding == null || !encoding.contains("aws-chunked")) {
                return rawBody;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int pos = 0;
            while (true) {
                int lineEnd = indexOfCrlf(pos);
                String sizeLine = new String(rawBody, pos, lineEnd - pos, StandardCharsets.US_ASCII);
                int size = Integer.parseInt(sizeLine.split(";")[0].trim(), 16);
                pos = lineEnd + 2;
                if (size == 0) {
                    break;
                }
                out.write(rawBody, pos, size);
                pos += size + 2;
            }
            String rest = new String(Arrays.copyOfRange(rawBody, pos, rawBody.length), StandardCharsets.US_ASCII);
            for (String line : rest.split("\r\n")) {
                if (!line.isEmpty() && !line.startsWith("x-amz-trailer-signature")) {
                    trailers.add(line);
                }
            }
            return out.toByteArray();
        }

        private int indexOfCrlf(int from) {
            for (int i = from; i < rawBody.length - 1; i++) {
                if (rawBody[i] == '\r' && rawBody[i + 1] == '\n') {
                    return i;
                }
            }
            throw new IllegalStateException("malformed aws-chunked body");
        }
    }
}
