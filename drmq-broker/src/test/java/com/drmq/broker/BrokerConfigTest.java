package com.drmq.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerConfigTest {

    @Test
    public void testConfigPropertiesLoading(@TempDir Path tempDir) throws IOException {
        File propsFile = tempDir.resolve("test.properties").toFile();
        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("port=9095\n");
            writer.write("data.dir=/tmp/drmq\n");
            writer.write("s3.archive.bucket=my-test-bucket\n");
            writer.write("raft.compact.threshold=5000\n");
        }

        String[] args = {
                "--config", propsFile.getAbsolutePath(),
                "--id", "node-5",
                "--port", "9099" // CLI override
        };

        BrokerConfig config = BrokerConfig.fromArgs(args);

        assertEquals("node-5", config.getNodeId(), "CLI should set id");
        assertEquals(9099, config.getPort(), "CLI port should override properties file port");
        assertEquals("/tmp/drmq", config.getDataDir(), "Should load data.dir from properties");
        assertEquals("my-test-bucket", config.getS3ArchiveBucket(), "Should load s3 bucket from properties");
        assertEquals(5000L, config.getRaftCompactThreshold(), "Should load raft threshold from properties");
    }
}
