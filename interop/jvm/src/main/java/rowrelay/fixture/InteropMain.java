package rowrelay.fixture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.protobuf.ProtobufDeserializerConfig;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Real official serializer and Kafka consumer, invoked by the Go integration fixture. */
public final class InteropMain {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final byte[] KEY = {0, (byte) 255, 127};
    private static final byte[] BINARY = {0, 1, (byte) 255};

    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("prepare|consume|consume-unavailable topic headers-mode exchange.json");
        String topic = args[1];
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverConfig.REGISTRY_URL, "http://127.0.0.1:28080/apis/registry/v3");
        config.put(SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID, "rowrelay-lab");
        config.put(SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, topic);
        config.put(SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, true);
        config.put(KafkaSerdeConfig.ENABLE_HEADERS, Boolean.parseBoolean(args[2]));
        config.put(SerdeConfig.DESERIALIZER_SPECIFIC_VALUE_RETURN_CLASS, Fixture.Change.class.getName());
        config.put(ProtobufDeserializerConfig.FALLBACK_ON_SCHEMA_ERROR, false);
        config.put(SchemaResolverConfig.RETRY_COUNT, 0);
        Path exchange = Path.of(args[3]);
        if (args[0].equals("prepare")) {
            List<Map<String, Object>> frames = new ArrayList<>();
            try (var serializer = new ProtobufKafkaSerializer<Fixture.Change>()) {
                serializer.configure(config, false);
                for (var operation : List.of(Fixture.Change.Operation.INSERT, Fixture.Change.Operation.UPDATE, Fixture.Change.Operation.DELETE)) {
                    String id = UUID.randomUUID().toString();
                    var headers = new RecordHeaders();
                    headers.add("duplicate", BINARY).add("duplicate", null).add("empty", new byte[0]);
                    byte[] value = serializer.serialize(topic, headers, expected(id, operation));
                    List<Map<String, Object>> orderedHeaders = new ArrayList<>();
                    for (var header : headers) {
                        Map<String, Object> h = new LinkedHashMap<>();
                        h.put("key", header.key());
                        h.put("value", header.value());
                        orderedHeaders.add(h);
                    }
                    frames.add(Map.of("id", id, "key", KEY, "value", value, "headers", orderedHeaders));
                }
            }
            JSON.writeValue(exchange.toFile(), frames);
        } else if (args[0].equals("consume") || args[0].equals("consume-unavailable")) {
            boolean unavailable = args[0].equals("consume-unavailable");
            if (unavailable) config.put(SchemaResolverConfig.REGISTRY_URL, "http://127.0.0.1:1/apis/registry/v3");
            var frames = JSON.readTree(Files.readAllBytes(exchange));
            config.put("bootstrap.servers", "127.0.0.1:29092");
            config.put("enable.auto.commit", false);
            config.put("auto.offset.reset", "none");
            config.put("isolation.level", "read_committed");
            config.put("allow.auto.create.topics", false);
            config.put("default.api.timeout.ms", 10000);
            config.put("key.deserializer", ByteArrayDeserializer.class);
            config.put("value.deserializer", ProtobufKafkaDeserializer.class);
            try (var consumer = new KafkaConsumer<byte[], Fixture.Change>(config)) {
                    var partition = new TopicPartition(topic, 0);
                    consumer.assign(List.of(partition));
                    consumer.seek(partition, 0);
                    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                    int received = 0;
                    while (received < frames.size() && System.nanoTime() < deadline) {
                        org.apache.kafka.clients.consumer.ConsumerRecords<byte[], Fixture.Change> records;
                        try {
                            records = consumer.poll(Duration.ofMillis(100));
                        } catch (SerializationException error) {
                            if (!unavailable) throw error;
                            System.out.println("PASS: unavailable registry blocks cold deserialization");
                            return;
                        }
                        for (var record : records) {
                            if (unavailable) throw new AssertionError("schema lookup failure was bypassed");
                            if (received >= frames.size()) throw new AssertionError("unexpected extra record");
                            var frame = frames.get(received);
                            var operation = Fixture.Change.Operation.forNumber(received + 1);
                            if (!expected(frame.get("id").asText(), operation).equals(record.value())) {
                                throw new AssertionError("typed protobuf images changed");
                            }
                            if (!Arrays.equals(KEY, record.key())) throw new AssertionError("binary key changed");
                            var id = record.headers().lastHeader("id");
                            if (id == null || !frame.get("id").asText().equals(new String(id.value(), StandardCharsets.UTF_8))) {
                                throw new AssertionError("delivery identity changed");
                            }
                            received++;
                        }
                    }
                    if (received != frames.size()) throw new AssertionError("incomplete committed delivery");
            }
            System.out.println("PASS: real JVM consumer decoded all typed INSERT/UPDATE/DELETE images");
        } else {
            throw new IllegalArgumentException("unknown fixture command");
        }
    }

    private static Fixture.Change expected(String id, Fixture.Change.Operation operation) {
        var image = Fixture.Image.newBuilder().setId(9007199254740993L).setOwnerId(7)
                .setAmount("12345678901234567890.12345").setLabel("café / Москва / 🐎")
                .setBinary(ByteString.copyFrom(BINARY)).build();
        var change = Fixture.Change.newBuilder().setEventId(id).setOperation(operation);
        if (operation != Fixture.Change.Operation.INSERT) change.setBefore(image);
        if (operation != Fixture.Change.Operation.DELETE) change.setAfter(image.toBuilder().clearOwnerId());
        return change.build();
    }
}
