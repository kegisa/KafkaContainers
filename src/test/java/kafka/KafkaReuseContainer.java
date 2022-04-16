package kafka;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaReuseContainer {
    private static Map<String, KafkaContainer> containers =
            new HashMap<>();

    public static KafkaContainer reuseContainer(String name) {
        if(containers.containsKey(name) && Objects.nonNull(containers.get(name))) {
            return containers.get(name);
        }
        KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        container.start();
        containers.put(name, container);
        return container;
    }
}
