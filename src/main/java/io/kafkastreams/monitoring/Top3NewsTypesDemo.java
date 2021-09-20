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
    public static Map<String, Map<Long, Long>> produceCounters = new HashMap<>();
    public static Map<String, Map<Long, Long>> observedCounts = new HashMap<>();
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
        properties.put("cache.max.bytes.buffering", 1024*1024L);
        properties.put("num.stream.threads", 20);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Top3Topology.withStreamsBuilder(streamsBuilder);

        startDataSimulation();
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            streams.close();
        }));
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
        Map<String, Iterator> iterators = new HashMap<>();
        for (int i = 0; i < categories.size(); i++) {
            produceCounters.put(categories.get(i).getNewsType(), new LinkedHashMap<>());
            observedCounts.put(categories.get(i).getNewsType(), new LinkedHashMap<>());
            iterators.put(categories.get(i).getNewsType(), produceCounters.get(categories.get(i).getNewsType()).keySet().iterator());
            dispatchers[i] = new Thread(new SImulator(producer, AppConfigs.inventoryTopic, AppConfigs.clicksTopic, categories.get(i), i, produceCounters.get(categories.get(i).getNewsType())));
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

        String csvReport = "";
        csvReport = csvReport + "second, category, produced, observed\n";
        boolean done = false;
        while (!done) {
            Long lowTs = System.currentTimeMillis();
            String lowCategory = null;
            for (int i = 0; i < categories.size(); i++) {
                Long val = null;
                Iterator<Long> iterator = iterators.get(i);
                if (iterators.get(i).hasNext()) {
                    val = iterator.next();
                }
                if (val != null && val < lowTs) {
                    lowTs = val;
                    lowCategory = categories.get(i).getNewsType();
                }
            }
            if (lowCategory == null) {
                done = true;
            } else {
                csvReport = csvReport + lowTs + ", " + lowCategory + ", " + produceCounters.get(lowCategory).get(lowTs) + ", " + observedCounts.get(lowCategory).get(lowTs) + "\n";
                for (int i = 0; i < categories.size(); i++) {
                    String category = categories.get(i).getNewsType();
                    if (!category.equals(lowCategory)) {
                        csvReport = csvReport + lowTs + ", " + category + ", " + produceCounters.get(category).get(lowTs) + ", " + observedCounts.get(category).get(lowTs) + "\n";
                    }
                }
            }
        }

        logger.info("Finished simulation.");
        logger.info("report:\n" + csvReport);
    }
}
