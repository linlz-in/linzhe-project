package com.userportraits;

import com.userportraits.bean.Common;
import com.userportraits.ods.ShopPermanceSource;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 15:57
 * @version: 1.8
 */
public class ShopPerformanceJob {
    @SneakyThrows
    public static void main(String[] args) {
        // 1. 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置实时特性
        env.enableCheckpointing(30000); // 30秒一次Checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(3); // 设置并行度（根据集群规模调整）

        // 3. 读取数据源（实时Kafka或本地模拟）
        DataStream<String> rawStream = ShopPermanceSource.buildSource(env);

        // 4. 执行ETL清洗
        DataStream<Common.CleanedShopData> cleanedStream = ShopPerformanceETL.processETL(rawStream);

        // 5. 实时指标计算
        DataStream<Common.ShopPerformanceResult> resultStream = ShopPerformanceADS.calculateIndicators(cleanedStream);

        // 6. 输出结果
        ShopPerformanceSink.writeResult(resultStream);

        // 7. 启动执行
        env.execute("Real-time Shop Performance Analysis");
    }
}
