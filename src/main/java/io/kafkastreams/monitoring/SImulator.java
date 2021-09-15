/*
 * Copyright (c) 2018. Prashant Kumar Pandey
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

import io.kafkastreams.monitoring.types.AdInventories;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runnable Kafka message dispatcher
 *
 * @author prashant
 * @author www.learningjournal.guru
 */
public class SImulator implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String adsTopic;
    private final String clicksTopic;
    private final AdInventories adInventories;
    private final int volumeFactor;
    private static final Logger logger = LogManager.getLogger(SImulator.class);
    private final static int internalFactor = 10;

    /**
     * A dispatcher thread takes a producer and sends Kafka messages to the given topic.
     * The Events are supplied in a file
     * @param producer     A valid producer instance
     * @param adsTopic
     * @param clicksTopic
     * @param adInventories
     * @param volumeFactor
     */
    SImulator(KafkaProducer<String, String> producer, String adsTopic, String clicksTopic, AdInventories adInventories, int volumeFactor) {
        this.producer = producer;
        this.adsTopic = adsTopic;
        this.clicksTopic = clicksTopic;
        this.adInventories = adInventories;
        this.volumeFactor = volumeFactor;
    }


    @Override
    public void run() {
        logger.info("Start processing " + adInventories.getInventoryID() + "...");
        Long start_time = System.currentTimeMillis();
        Long volumeSent = 0L;

        try {
            Thread.sleep(4000 + volumeFactor * 2000);
            String category = "{\"ts\": " + System.currentTimeMillis() + ",  \"InventoryID\": \"" + adInventories.getInventoryID() + "\", \"NewsType\": \"" + adInventories.getNewsType() + "\"}";
            producer.send(new ProducerRecord<String, String>(adsTopic, adInventories.getInventoryID(), category));
            for (int i = 0; i < 20; i++) {
                long iterationStart = System.currentTimeMillis();
                for (int i1 = 0; i1 < volumeToSend(i, 20); i1++) {
                    Long ts = System.currentTimeMillis();
                    Top3NewsTypesDemo.lastProduced.set(ts);
                    String clickEvent = "{\"ts\": " + ts + ",  \"InventoryID\": \"" + adInventories.getInventoryID() + "\"}";
                    producer.send(new ProducerRecord<String, String>(clicksTopic, adInventories.getInventoryID(), clickEvent));
                    volumeSent++;
                    Top3NewsTypesDemo.producedPerCategory.put(adInventories.getNewsType(), volumeSent);
                    Thread.sleep(8000  / volumeToSend(i, 20));
                }
                logger.info("Finished sending " + volumeSent + " messages for  " + adInventories.getInventoryID() + ", " + adInventories.getNewsType());
                while (System.currentTimeMillis()-iterationStart < 10000) Thread.sleep(30);
                logger.info("Awake ("+  adInventories.getInventoryID() + ", " + adInventories.getNewsType() + ")");
            }
        } catch (Exception e) {
            logger.error("Exception in thread " + adInventories.getInventoryID());
            throw new RuntimeException(e);
        }
        Long end_time = System.currentTimeMillis();
        logger.info("Sent " + volumeSent + " " + adInventories.getNewsType() + " took : " + (end_time - start_time));
    }

    private int volumeToSend(int iteration, int iterations) {
        return iteration+1 < iterations / 2 ? internalFactor * volumeFactor * (iteration + 1) : iteration+1 < iterations * 3 / 4 ? internalFactor * 20 * (11-volumeFactor) * (iterations - iteration) : internalFactor * 4 * (11-(11-volumeFactor)) * (iterations - iteration);
    }
}
