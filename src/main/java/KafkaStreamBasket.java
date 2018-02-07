import serde.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import rpc.KafkaStateServer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by octo-art on 15/05/2017.
 */
public class KafkaStreamBasket {

    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";


    public static void main(String... args) throws Exception {

        KStreamBuilder builder = new KStreamBuilder();

        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-basket");
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + String.valueOf(KafkaStateServer.APPLICATION_SERVER_PORT));
        settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<String> stringSerde = Serdes.String();
        final Serde<prices> specificAvroSerde = new SpecificAvroSerde<>();
        // Note how we must manually call `configure()` on this com.octo.serde to configure the schema registry
        // url.  This is different from the case of setting default serdes (see `streamsConfiguration`
        // above), which will be auto-configured based on the `StreamsConfiguration` instance.
        final boolean isKeySerde = false;

        specificAvroSerde.configure(
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL),
                isKeySerde);

        StreamsConfig config = new StreamsConfig(settings);

        KStream<String, prices> prices = builder.stream("confluent-in-prices");

        prices.map((key, value) -> new KeyValue<String, Long>(value.getItem().toString(), value.getPrice().longValue()))
                //Specifiying the Serdes is essential if they differ from the settings default serdes which is our case
                .groupByKey(Serdes.String(),
                    Serdes.Long()
                )
                .aggregate(
                    () -> 0L, // initializer
                    (aggKey, newValue, aggValue) -> aggValue + newValue, // adder
                    Serdes.Long(), // com.octo.serde for aggregate value
                "basket-store");

        prices.to(stringSerde, specificAvroSerde,"confluent-out-prices");

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        KafkaStateServer.run(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
