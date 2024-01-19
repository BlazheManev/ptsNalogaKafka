package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Main {

    private final static String TOPIC = "kafka-racun";

    private static KafkaProducer<String, GenericRecord> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer<>(props);
    }

    private static ProducerRecord<String, GenericRecord> generateRecord(Schema schema) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        int idRacun = rand.nextInt(1000);
        int receptaIdRacun = rand.nextInt(100);
        int zaposlenIdZaposlen = rand.nextInt(100);
        String znesek = String.format("%.2f", rand.nextDouble() * 1000);
        long datum = System.currentTimeMillis();

        avroRecord.put("idRacun", idRacun);
        avroRecord.put("Recepta_idRacun", receptaIdRacun);
        avroRecord.put("Zaposlen_idZaposlen", zaposlenIdZaposlen);
        avroRecord.put("Znesek", znesek);
        avroRecord.put("Datum", datum);

        return new ProducerRecord<>(TOPIC, String.valueOf(idRacun), avroRecord);
    }

    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("Racun")
                .fields()
                .requiredInt("idRacun")
                .requiredInt("Recepta_idRacun")
                .requiredInt("Zaposlen_idZaposlen")
                .requiredString("Znesek")
                .requiredLong("Datum")
                .endRecord();

        KafkaProducer<String, GenericRecord> producer = createProducer();

        while(true){
            ProducerRecord<String, GenericRecord> record = generateRecord(schema);
            producer.send(record);

            System.out.println("[RECORD] Sent new Racun record.");
            Thread.sleep(10000);
        }
    }
}
