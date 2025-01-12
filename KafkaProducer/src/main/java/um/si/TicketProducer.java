package um.si;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class TicketProducer {

    // Tema, na katero bomo pošiljali sporočila
    private final static String TOPIC = "theatre-tickets";

    // Ustvari Kafka producer s potrebnimi nastavitvami
    private static KafkaProducer<String, GenericRecord> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TheatreTicketProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new KafkaProducer<>(props);
    }

    // Funkcija za generiranje naključnega zapisa
    private static ProducerRecord<String, GenericRecord> generateRecord(Schema schema) {
        Random rand = new Random();

        // Seznami različnih vrednosti za generiranje
        List<String> firstNames = Arrays.asList("John", "Alice", "Michael", "Sarah", "David", "Emma");
        List<String> lastNames = Arrays.asList("Doe", "Smith", "Johnson", "Brown", "Davis", "Wilson");
        List<String> addresses = Arrays.asList("123 Elm Street", "456 Oak Avenue", "789 Pine Road", "101 Maple Drive");
        List<String> subscriptions = Arrays.asList("Premium", "Standard", "VIP", "Basic");
        List<String> paymentMethods = Arrays.asList("CreditCard", "PayPal", "Cash", "DebitCard");

        // Naključno izbira vrednosti iz seznamov
        String firstName = firstNames.get(rand.nextInt(firstNames.size()));
        String lastName = lastNames.get(rand.nextInt(lastNames.size()));
        String address = addresses.get(rand.nextInt(addresses.size()));
        String subscription = subscriptions.get(rand.nextInt(subscriptions.size()));
        String paymentMethod = paymentMethods.get(rand.nextInt(paymentMethods.size()));

        // Generira nov zapis za karto
        GenericRecord ticketRecord = new GenericData.Record(schema);
        ticketRecord.put("firstName", firstName);
        ticketRecord.put("lastName", lastName);
        ticketRecord.put("address", address);
        ticketRecord.put("subscription", subscription);
        ticketRecord.put("seat", "A" + (rand.nextInt(20) + 1)); // Naključno izbira sedež
        ticketRecord.put("ticketNumber", "T" + (rand.nextInt(10000) + 1)); // Naključno generira številko karte
        ticketRecord.put("price", rand.nextFloat() * 50 + 10); // Naključno generira ceno
        ticketRecord.put("paymentMethod", paymentMethod);

        return new ProducerRecord<>(TOPIC, ticketRecord.get("ticketNumber").toString(), ticketRecord);
    }

    // Glavna metoda za pošiljanje podatkov v Kafka
    public static void main(String[] args) throws InterruptedException {
        // Shema za zapis v Kafka (opis strukture podatkov)
        Schema schema = SchemaBuilder.record("TheaterTicket")
                .namespace("com.gledaliska.blagajna")
                .fields()
                .requiredString("firstName") // Ime
                .requiredString("lastName") // Priimek
                .requiredString("address") // Naslov
                .requiredString("subscription") // Vrsta naročnine
                .requiredString("seat") // Sedež
                .requiredString("ticketNumber") // Številka karte
                .requiredFloat("price") // Cena
                .requiredString("paymentMethod") // Način plačila
                .endRecord();

        // Ustvari Kafka producer
        KafkaProducer<String, GenericRecord> producer = createProducer();

        // Neskončen zanko za pošiljanje zapisov
        while (true) {
            // Generira nov zapis
            ProducerRecord<String, GenericRecord> record = generateRecord(schema);
            // Pošlje zapis v Kafka
            producer.send(record);
            System.out.println("[RECORD] Sent new theater ticket: " + record.value());
            Thread.sleep(5000);  // Počakaj 5 sekund pred pošiljanjem naslednjega zapisa
        }
    }
}
