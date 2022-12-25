package com.flink.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingPipeline {

    public static void main(String[] args) throws Exception {

        String inputTopic = "";
        String outputTopic = "";
        String bootstrapServers = "";
        String groupId = "";
        String clusterAddress = "";

        Thread.currentThread().setContextClassLoader(null);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // FlinkKafkaConsumer is deprecated, using KafkaSource instead
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(groupId)
                .setTopics(inputTopic)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "false")
                .setProperty("auto.offset.reset", "earliest")
                .build();

        DataStream<String> stream = environment.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource")
                .returns(Types.STRING);

//        stream.print();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                        .setTopic(outputTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                                .build();

        stream.sinkTo(sink);

        environment.execute("StreamingPipeline");

    }
}
