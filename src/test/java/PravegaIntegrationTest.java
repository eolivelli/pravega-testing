/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import static org.junit.Assert.assertEquals;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class PravegaIntegrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final String SCOPE = "zop";
    private static final String STREAMNAME = "mystream";
    private static final URI CONTROLLERURI = URI.create("tcp://localhost:8989");

    @Test
    public void test() throws Exception {
        ServiceBuilderConfig config = ServiceBuilderConfig
                .builder()
                .include(System.getProperties())
                .build();

        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(8989)
                .segmentStorePort(8990)
                .zkPort(8991)
                .enableRestServer(false)
                .enableAuth(false)
                .enableTls(false);

        try (LocalPravegaEmulator localPravega = emulatorBuilder.build();) {
            Method startMethod = localPravega.getClass().getDeclaredMethod("start");
            startMethod.setAccessible(true);
            startMethod.invoke(localPravega);
            // localPravega.start();

            try (StreamManager streamManager = StreamManager.create(CONTROLLERURI);) {
                streamManager.createScope(SCOPE);

                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .build();

                streamManager.createStream(SCOPE, STREAMNAME, streamConfig);
                try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE,
                        ClientConfig.builder().controllerURI(CONTROLLERURI).build());) {

                    final String readerGroup = UUID.randomUUID().toString().replace("-", "");
                    final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                            .stream(Stream.of(SCOPE, STREAMNAME))
                            .build();

                    List<CompletableFuture<Void>> futures = new ArrayList<>();
                    try (EventStreamWriter<byte[]> writer =
                            clientFactory.createEventWriter(STREAMNAME, new ByteArraySerializer(), EventWriterConfig.builder().build());) {
                        for (int i = 0; i < 100; i++) {
                            CompletableFuture<Void> handle = writer.writeEvent("foo".getBytes("utf-8"));
                            futures.add(handle);
                        }
                    }

                    CompletableFuture.allOf(futures.toArray(a->new CompletableFuture[a])).join();

                    try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, CONTROLLERURI)) {
                        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
                    }
                    try (EventStreamReader<byte[]> consumer = clientFactory.createReader("reader", readerGroup,
                            new ByteArraySerializer(), ReaderConfig.builder().build());) {
                        List<String> results = new ArrayList<>();
                        for (int i = 0; i < 100; i++) {
                            EventRead<byte[]> readNextEvent = consumer.readNextEvent(1000);
                            System.out.println("RECEIVED "+readNextEvent);
                            if (readNextEvent != null
                                     && !readNextEvent.isCheckpoint()
                                     && readNextEvent.getEvent() != null
                                    ) {
                                String event = new String(readNextEvent.getEvent(), "utf-8");
                                assertEquals("foo", event);
                                results.add(event);
                            }
                            if (results.size() == futures.size()) {
                                break;
                            }
                        }
                        assertEquals(results.size(), futures.size());
                    }
                }
            }
        }
    }
}
