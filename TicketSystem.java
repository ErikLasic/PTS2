package um.si;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

public class TicketStreamer {

    private static Properties setupApp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "TicketAnalysis");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://0.0.0.0:8081");
        return props;
    }

    public static void main(String[] args) throws Exception {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://0.0.0.0:8081");

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        // Preberi in obdelaj temo "kafka-karte"
        KStream<Integer, GenericRecord> karteStream = builder.stream("kafka-karte", Consumed.with(Serdes.Integer(), valueGenericAvroSerde));

        // 1. Filtriranje: Abonenti, ki so kupili več kot 5 kart
        KStream<String, Integer> abonentiWithMoreThan5Tickets = karteStream
                .map((key, value) -> new KeyValue<>(value.get("abonent_id").toString(), 1)) // Vsaka karta je ena enota
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Skupini po abonentu
                .reduce(Integer::sum) // Agregira število kart po abonentu
                .toStream();

        abonentiWithMoreThan5Tickets
                .filter((key, value) -> value > 5) // Filtriraj abonente, ki imajo več kot 5 kart
                .foreach((key, value) -> System.out.println("Abonent z več kot 5 kartami: " + key + " - Total tickets: " + value));

        // 2. Agregacija: Število kart po predstavi
        KStream<String, Integer> ticketsPerShow = karteStream
                .map((key, value) -> new KeyValue<>(value.get("predstava_id").toString(), 1)) // Vsaka karta je ena enota
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Skupini po predstavi
                .reduce(Integer::sum) // Agregira število kart za vsako predstavo
                .toStream();

        ticketsPerShow.foreach((key, value) -> System.out.println("Predstava ID: " + key + " - Total tickets sold: " + value));

        // Zgradimo in zaženemo aplikacijo
        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, setupApp());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
