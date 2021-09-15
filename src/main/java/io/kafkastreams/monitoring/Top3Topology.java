package io.kafkastreams.monitoring;

import io.kafkastreams.monitoring.common.AppConfigs;
import io.kafkastreams.monitoring.common.AppSerdes;
import io.kafkastreams.monitoring.common.Top3NewsTypes;
import io.kafkastreams.monitoring.types.AdClick;
import io.kafkastreams.monitoring.types.AdInventories;
import io.kafkastreams.monitoring.types.ClicksByNewsType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Top3Topology {
    private static final Logger logger = LogManager.getLogger();

    public static Topology withStreamsBuilder(StreamsBuilder streamsBuilder) {
        //Global table for all inventories
        GlobalKTable<String, AdInventories> adInventoriesGlobalKTable =
                streamsBuilder.globalTable(
                        AppConfigs.inventoryTopic,
                        Consumed.with(AppSerdes.String(),
                                AppSerdes.AdInventories())
                );

        //Stream of Clicks
        KStream<String, AdClick> clicksKStream =
                streamsBuilder.stream(AppConfigs.clicksTopic,
                        Consumed.with(AppSerdes.String(),
                                AppSerdes.AdClick())
                );

        //Stream of Clicked Inventories
        KStream<String, AdInventories> clickedInventoryKStream =
                clicksKStream.join(
                        adInventoriesGlobalKTable,
                        (clickKey, clickValue) -> clickKey,
                        (adClick, adInventory) -> adInventory
                );

        //Group clicked inventories on NewsType and count
        //You will get clicks by news Types
        KTable<String, Long> clicksByNewsTypeKTable =
                clickedInventoryKStream.groupBy(
                        (inventoryID, inventoryValue) -> inventoryValue.getNewsType(),
                        Grouped.with(AppSerdes.String(), AppSerdes.AdInventories())
                ).count();

//        clicksKStream.peek((s, adClick) -> {
//            if (adClick.getTs() > lastConsumed.get()) {
//                lastConsumed.set(adClick.getTs());
//            }
//        });

        //The clicksByNewsType is exhaustive and distributed
        //We need to compute the top 3 only
        //In order to do that, we must following
        //1. Bring all clicksByNewsType to a single partition
        //2. Sort them by clicks and take only top 3
        //There are two steps to achieve this.
        //1. Group them on a common key to bring it to a single partition
        //2. Simulate Sort()+Top(3) using a custom aggregation
        KTable<String, Top3NewsTypes> top3NewsTypesKTable =
                clicksByNewsTypeKTable.groupBy(
                        (k_newsType, v_clickCount) -> {
                            ClicksByNewsType value = new ClicksByNewsType();
                            value.setNewsType(k_newsType);
                            value.setClicks(v_clickCount);
                            return KeyValue.pair(AppConfigs.top3AggregateKey, value);
                        },
                        Grouped.with(AppSerdes.String(), AppSerdes.ClicksByNewsType())
                ).aggregate(Top3NewsTypes::new,
                        (k, newClickByNewsType, aggTop3NewType) -> {
                            aggTop3NewType.add(newClickByNewsType);
                            return aggTop3NewType;
                        },
                        (k, oldClickByNewsType, aggTop3NewType) -> {
                            aggTop3NewType.remove(oldClickByNewsType);
                            return aggTop3NewType;
                        },
                        Materialized.<String, Top3NewsTypes, KeyValueStore<Bytes, byte[]>>
                                as("top3-clicks")
                                .withKeySerde(AppSerdes.String())
                                .withValueSerde(AppSerdes.Top3NewsTypes()));

        top3NewsTypesKTable.toStream().foreach((k, v) -> {
            try {
                logger.info("top 3 categories=" + Top3NewsTypesDemo.top3(v));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return null;
    }

}
