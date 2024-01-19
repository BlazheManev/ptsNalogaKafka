package org.example;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.avro.generic.GenericRecord;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class Main {

    private static Properties setupApp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "racun-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("default.deserialization.exception.handler", "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        return props;
    }

    public static void main(String[] args) throws Exception {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        String inputTopic = "kafka-racun";
        KStream<String, GenericRecord> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), valueGenericAvroSerde));

        // Format for the date
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        // 1. Counting records per day
        inputStream.map((key, value) -> new KeyValue<>(sdf.format(new Date((long) value.get("Datum"))), "1"))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream()
                .to("kafka-racun-count-per-day", Produced.with(Serdes.String(), Serdes.Long()));

        // 2. Counting records by Zaposlen_idZaposlen per day
        inputStream.map((key, value) -> new KeyValue<>(value.get("Zaposlen_idZaposlen").toString() + "-" + sdf.format(new Date((long) value.get("Datum"))), "1"))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream()
                .to("kafka-racun-count-by-zaposlen-per-day", Produced.with(Serdes.String(), Serdes.Long()));

        // 3. Sum the 'Znesek' values for each 'Zaposlen_idZaposlen'
        inputStream.map((k, v) -> new KeyValue<>(v.get("Zaposlen_idZaposlen").toString(), Double.valueOf(v.get("Znesek").toString())))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce(Double::sum)
                .toStream()
                .to("kafka-summed-znesek-per-zaposlen", Produced.with(Serdes.String(), Serdes.Double()));

        inputStream.print(Printed.toSysOut());

        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, setupApp());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
