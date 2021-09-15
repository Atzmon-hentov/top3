/*
 * Copyright (c) 2019. Prashant Kumar Pandey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package io.kafkastreams.monitoring;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafkastreams.monitoring.common.AppConfigs;
import io.kafkastreams.monitoring.common.Top3NewsTypes;
import io.kafkastreams.monitoring.types.AdInventories;
import io.kafkastreams.monitoring.types.ClicksByNewsType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demo KStream-GlobalKTable join and TopN aggregation
 *
 * @author prashant
 * @author www.learningjournal.guru
 */
public class Top3NewsTypesDemo {
    private static final Logger logger = LogManager.getLogger();
    public static AtomicLong lastProduced = new AtomicLong(0L);
    public static AtomicLong lastConsumed = new AtomicLong(0L);
    public static Map<String, Long> producedPerCategory = new ConcurrentHashMap<>();
    public static Map<String, Long> maxLagPerPartition = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
                AppConfigs.applicationID);
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "Top1Playground");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                AppConfigs.bootstrapServers);
        properties.put(StreamsConfig.STATE_DIR_CONFIG,
                AppConfigs.stateStoreName);
        properties.put("state.cleanup.delay.ms", 3600000);
        properties.put("commit.interval.ms", 50);
        properties.put("num.stream.threads", 20);

//        properties.put(StreamsConfig.RECEIVE_BUFFER_CONFIG, 20000);
//        properties.put("cache.max.bytes.buffering", 200000);
//        properties.put("buffered.records.per.partition", 1);
//        properties.put("topology.optimization", "all");


        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Top3Topology.withStreamsBuilder(streamsBuilder);
        //Global table for all inventories
//        GlobalKTable<String, AdInventories> adInventoriesGlobalKTable =
//                streamsBuilder.globalTable(
//                        AppConfigs.inventoryTopic,
//                        Consumed.with(AppSerdes.String(),
//                                AppSerdes.AdInventories())
//                );
//
//        //Stream of Clicks
//        KStream<String, AdClick> clicksKStream =
//                streamsBuilder.stream(AppConfigs.clicksTopic,
//                        Consumed.with(AppSerdes.String(),
//                                AppSerdes.AdClick())
//                );
//
//        //Stream of Clicked Inventories
//        KStream<String, AdInventories> clickedInventoryKStream =
//                clicksKStream.join(
//                        adInventoriesGlobalKTable,
//                        (clickKey, clickValue) -> clickKey,
//                        (adClick, adInventory) -> adInventory
//                );
//
//        //Group clicked inventories on NewsType and count
//        //You will get clicks by news Types
//        KTable<String, Long> clicksByNewsTypeKTable =
//                clickedInventoryKStream.groupBy(
//                        (inventoryID, inventoryValue) -> inventoryValue.getNewsType(),
//                        Grouped.with(AppSerdes.String(), AppSerdes.AdInventories())
//                ).count();
//
////        clicksKStream.peek((s, adClick) -> {
////            if (adClick.getTs() > lastConsumed.get()) {
////                lastConsumed.set(adClick.getTs());
////            }
////        });
//
//        //The clicksByNewsType is exhaustive and distributed
//        //We need to compute the top 3 only
//        //In order to do that, we must following
//        //1. Bring all clicksByNewsType to a single partition
//        //2. Sort them by clicks and take only top 3
//        //There are two steps to achieve this.
//        //1. Group them on a common key to bring it to a single partition
//        //2. Simulate Sort()+Top(3) using a custom aggregation
//        KTable<String, Top3NewsTypes> top3NewsTypesKTable =
//                clicksByNewsTypeKTable.groupBy(
//                        (k_newsType, v_clickCount) -> {
//                            ClicksByNewsType value = new ClicksByNewsType();
//                            value.setNewsType(k_newsType);
//                            value.setClicks(v_clickCount);
//                            return KeyValue.pair(AppConfigs.top3AggregateKey, value);
//                        },
//                        Grouped.with(AppSerdes.String(), AppSerdes.ClicksByNewsType())
//                ).aggregate(Top3NewsTypes::new,
//                        (k, newClickByNewsType, aggTop3NewType) -> {
//                            aggTop3NewType.add(newClickByNewsType);
//                            return aggTop3NewType;
//                        },
//                        (k, oldClickByNewsType, aggTop3NewType) -> {
//                            aggTop3NewType.remove(oldClickByNewsType);
//                            return aggTop3NewType;
//                        },
//                        Materialized.<String, Top3NewsTypes, KeyValueStore<Bytes, byte[]>>
//                                as("top3-clicks")
//                                .withKeySerde(AppSerdes.String())
//                                .withValueSerde(AppSerdes.Top3NewsTypes()));
//
//        top3NewsTypesKTable.toStream().foreach((k, v) -> {
//            try {
//                logger.info("top 3 categories=" + top3(v));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            streams.close();
        }));
        startDataSimulation();
    }


    public static String top3(Top3NewsTypes top3NewsTypes) throws JsonProcessingException {
        String result = "";
        int i = 1;
        for (ClicksByNewsType clicksByNewsType : top3NewsTypes.t3sorted) {
            Long produced = Top3NewsTypesDemo.producedPerCategory.get(clicksByNewsType.getNewsType());
            if (produced == null) produced = 0L;
            Long lag = produced - clicksByNewsType.getClicks();
            Long maxLag = maxLagPerPartition.get(clicksByNewsType.getNewsType());
            if (maxLag == null ||  lag > maxLag) maxLagPerPartition.put(clicksByNewsType.getNewsType(), lag);
            maxLag = maxLagPerPartition.get(clicksByNewsType.getNewsType());
            result = result + i++ + ": (" + clicksByNewsType.getNewsType() + "=" + clicksByNewsType.getClicks() + ", lag=" + lag + ", max-lag=" + maxLag + ")";
        }

        return result;
    }
    private static void startDataSimulation() {
        logger.trace("Starting dispatcher threads...");
        final String kafkaConfig = "/kafka.properties";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.put("client.id", "DataSimulator");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        List<AdInventories> categories = new ArrayList<>();

        categories.add(AdInventories.newInstance("1001", "Sports"));
        categories.add(AdInventories.newInstance("1002", "Politics"));
        categories.add(AdInventories.newInstance("1003", "LocalNews"));
        categories.add(AdInventories.newInstance("1004", "WorldNews"));
        categories.add(AdInventories.newInstance("1005", "Health"));
        categories.add(AdInventories.newInstance("1006", "Lifestyle"));
        categories.add(AdInventories.newInstance("1007", "Literature"));
        categories.add(AdInventories.newInstance("1008", "Education"));
        categories.add(AdInventories.newInstance("1009", "Social"));
        categories.add(AdInventories.newInstance("1010", "Business"));

        Thread[] dispatchers = new Thread[categories.size()];
        for (int i = 0; i < categories.size(); i++) {
            dispatchers[i] = new Thread(new SImulator(producer, AppConfigs.inventoryTopic, AppConfigs.clicksTopic, categories.get(i), i));
            dispatchers[i].start();
        }

        long start_time = System.currentTimeMillis();
        try {
            for (Thread t : dispatchers)
                t.join();
        } catch (InterruptedException e) {
            logger.error("Thread Interrupted ");
        } finally {
            logger.info("Finished dispatcher demo - Closing Kafka Producer.");
            producer.close();
            Long end_time = System.currentTimeMillis();
            logger.info("took: " + (end_time - start_time) + " msec");
        }
    }
}
