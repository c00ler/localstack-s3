package com.github.avenderov;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.LogMessageWaitStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Alexey Venderov
 */
public class LocalstackS3IT {

    private static final Logger LOG = LoggerFactory.getLogger(LocalstackS3IT.class);

    private static final int LOCALSTACK_S3_PORT = 4572;

    private static final String REGION = Regions.EU_CENTRAL_1.getName();

    private static final String TEST_BUCKET = "test-bucket";

    @ClassRule
    public static GenericContainer localstack =
            new GenericContainer("atlassianlabs/localstack:0.6.0")
                    .withEnv("SERVICES", "s3")
                    .withEnv("DEFAULT_REGION", REGION)
                    .withExposedPorts(LOCALSTACK_S3_PORT)
                    .waitingFor((new LogMessageWaitStrategy()
                            .withRegEx(".*Ready\\.\n").withStartupTimeout(Duration.ofSeconds(10L))));

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    private AmazonS3 amazonS3;

    private File testFile;

    @Before
    public void beforeEach() throws IOException {
        final String serviceEndpoint = String.format("http://%s:%d",
                localstack.getContainerIpAddress(), localstack.getMappedPort(LOCALSTACK_S3_PORT));

        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("LocalStackDummyAccessKey", "LocalStackDummySecretKey")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, REGION))
                .withPathStyleAccessEnabled(true)
                .withChunkedEncodingDisabled(true)
                .build();

        final Bucket bucket = amazonS3.createBucket(TEST_BUCKET);

        LOG.info("Test bucket created: {}", bucket.getName());

        testFile = tempFolder.newFile("test.txt");
        Files.write(testFile.toPath(), "localstack".getBytes(StandardCharsets.UTF_8));

        LOG.info("Test file created: {}", testFile.getAbsolutePath());
    }

    @Test
    public void shouldListObjects() throws IOException {
        final String prefix = "folder/";
        final Collection<String> expectedKeys = uploadFiles(prefix);

        final ObjectListing objects = amazonS3.listObjects(TEST_BUCKET, prefix);

        assertThat(objects.getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .containsAll(expectedKeys);
    }

    @Test
    public void shouldListObjectsUsingApiV2() throws IOException {
        final String prefix = "folder/";
        final Collection<String> expectedKeys = uploadFiles(prefix);

        final ListObjectsV2Result objects = amazonS3.listObjectsV2(TEST_BUCKET, prefix);

        assertThat(objects.getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .containsAll(expectedKeys);
    }

    private Collection<String> uploadFiles(final String prefix) {
        return IntStream.range(0, 3)
                .boxed()
                .map(i -> {
                    final String key = prefix + UUID.randomUUID().toString() + ".txt";
                    amazonS3.putObject(TEST_BUCKET, key, testFile);
                    return key;
                })
                .peek(k -> LOG.info("File uploaded. Key: {}", k))
                .collect(Collectors.toList());
    }

}
