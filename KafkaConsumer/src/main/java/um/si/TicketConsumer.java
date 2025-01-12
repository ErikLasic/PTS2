package um.si;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class TicketConsumer {

    // Tema, iz katere bomo brali sporočila
    private final static String TOPIC = "theatre-tickets";

    // Ustvari Kafka consumer s potrebnimi nastavitvami
    private static KafkaConsumer<String, GenericRecord> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "theatre-ticket-consumers");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Začetek branja od prvega sporočila
        return new KafkaConsumer<>(props);
    }

    // Glavna metoda za branje podatkov iz Kafka
    public static void main(String[] args) {
        // Ustvari Kafka consumer
        KafkaConsumer<String, GenericRecord> consumer = createConsumer();
        // Prijavi se na temo
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                // Prebere sporočila iz Kafka
                ConsumerRecords<String, GenericRecord> records = consumer.poll(1000); // Čaka na sporočila
                // Obdelava vsakokratnega sporočila
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    // Izpiše podatke sporočila
                    System.out.printf(
                            "Consumed record - Offset: %d, Key: %s, Value: %s%n",
                            record.offset(), record.key(), record.value()
                    );
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming records: " + e.getMessage());
        } finally {
            // Zapre consumer na koncu
            consumer.close();
        }
    }
}
