package com.userportraits.ods;

import com.alibaba.fastjson.JSON;
import com.userportraits.bean.Common;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 9:38
 * @version: 1.8
 *
 * Source: 数据源接入 -- 模拟数据/真实kafka数据
 */
public class ShopPermanceSource {
    /**
     * 构建数据流
     */
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(Common.DATETIME_FORMAT);
    private static final Random random = new Random();
    public static DataStream<String> buildSource(StreamExecutionEnvironment env) {
        // 示例：通过系统属性/配置判断环境，实际可从配置中心、枚举等灵活获取
        String envFlag = System.getProperty("env", "prod");
        if ("prod".equals(envFlag)) {
            // 本地测试：模拟数据实时数据
            return env.addSource(new SourceFunction<String>() {
                private boolean isRunning = true;
                @SneakyThrows
                @Override
                public void run(SourceContext<String> ctx) {
                    String[]  productIds = {"prod_001", "prod_002", "prod_003", "UNIDENTIFIED"};
                    String[] dimensions = {"店铺","客服","静默"};
                    int count = 0;

                    //假如生成7天数据
                    int totalDays = 7;
                    while (isRunning && count < totalDays * 1000 ){ //&& count < totalDays * 1000
                        // 模拟网络延迟导致的乱序 0-3秒随机延迟
                        Thread.sleep(random.nextInt(100));

                        RawShopDate data = new RawShopDate();

                        data.setRecordId("rec_" + System.currentTimeMillis());
                        data.setUserId("user_" + random.nextInt(100));
                        data.setProductId(productIds[random.nextInt(productIds.length)]);
                        data.setCustomerServiceId(random.nextBoolean() ? "cs_" + random.nextInt(50) : null);
                        data.setDimension(dimensions[random.nextInt(dimensions.length)]);

                        // 核心修改：随机生成最近N天内的咨询时间（按天分布）
                        int randomDays = random.nextInt(totalDays); // 0到6天前
//                        int randomDays = 0;
                        LocalDateTime baseDate = LocalDateTime.now().minusDays(randomDays)
                                .withHour(random.nextInt(12) + 9) // 集中在每天9:00-21:00
                                .withMinute(0).withSecond(0).withNano(0); // 重置到整点，方便按天聚合
                        // 在当天内随机生成秒数（0-12小时）
                        LocalDateTime consultTime = baseDate.plusSeconds(random.nextInt( 3600));
                        data.setConsultTime(consultTime.format(dtf));

                        if (random.nextDouble() < 0.7){ //70%概率询单
                            LocalDateTime inquireTime = consultTime.plusSeconds(random.nextInt(60));
                            data.setInquireTime(inquireTime.format(dtf));

                            if (random.nextDouble() < 0.3){ //30%概率下单
                                LocalDateTime orderTime = inquireTime.plusSeconds(random.nextInt(120));
                                data.setPlaceOrderTime(orderTime.format(dtf));

                                if (random.nextDouble() < 0.8){ //80%概率付款
                                    LocalDateTime payTime = orderTime.plusSeconds(random.nextInt(300));
                                    data.setPayTime(payTime.format(dtf));
                                    data.setPayAmount(100 + random.nextDouble() * 900); // 100-1000
                                    data.setPayQuantity(1 + random.nextInt(5)); // 1-5件
                                }
                            }

                        }
                        ctx.collect(JSON.toJSONString(data));
                        count++;
                    }
                }

                @Override
                public void cancel() {
                    isRunning = false;
                }
            }).name("local-simulated-source");
        } else {
            // 生产环境：Kafka 数据源
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", "cdh01:9092,cdh02:9092,cdh03:9092");
            kafkaProps.setProperty("group.id", "shop-performance-group");
            kafkaProps.setProperty("auto.offset.reset", "latest");
            kafkaProps.setProperty("max.poll.records", "200");
            kafkaProps.setProperty("heartbeat.interval.ms", "5000");
            kafkaProps.setProperty("session.timeout.ms", "30000");

            return env.addSource(new FlinkKafkaConsumer<>(
                            "shop-performance-raw-data",
                            new SimpleStringSchema(),
                            kafkaProps))
                    .setParallelism(4)
                    .name("kafka-real-time-source");
        }
    }

    //用于模拟数据的原始数据类
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class RawShopDate {
        private String recordId;
        private String userId;
        private String productId;
        private String customerServiceId;
        private String dimension;
        private String consultTime;
        private String inquireTime;
        private String placeOrderTime;
        private String payTime;
        private Double payAmount;
        private Integer payQuantity;
    }
}
