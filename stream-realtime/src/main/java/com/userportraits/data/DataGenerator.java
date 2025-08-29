package com.userportraits.data;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 8:59
 * @version: 1.8
 *
 * 店铺绩效模拟数据生成器： 生成2000条全链路业务数据
 */
public class DataGenerator implements SourceFunction<String> {
    private boolean isRunning = true;
    private int currentCount = 0;
    private final Random random = new Random();
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 模拟维度、渠道、消息类型
    private static final String[] DIMNSIONS = {"店铺", "客服", "静默"};
    private static final String[] ENTRY_CHANNELS = {"商品详情页", "订单详情页", "主动咨询"};
    private static final String[] MESSAGE_TYPES = {"长链接", "商品卡片", "短链接", "图片", "二维码"};

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //数据总数量
        int totalCount = 2000;
        while (isRunning && currentCount < totalCount) {
            // 先构造单条数据
            ShopPerformanceData data = new ShopPerformanceData();
            data.setRecorId("rec_" + String.format("%06d", currentCount+1));
            data.setUserId("user_" + (1000 + random.nextInt(500))); //500个不同用户
            data.setProductId("prod_" + (2000 + random.nextInt(200))); //200个不同商品
            data.setProductName("商品" + (2000 + random.nextInt(200)));
            data.setCustomerServiceId("cs_" + (3000 + random.nextInt(50))); //50个不同客服
            data.setCsName("客服" + (char) ('A' + random.nextInt(26)));
            data.setEntryChannel(ENTRY_CHANNELS[random.nextInt(ENTRY_CHANNELS.length)]);
            data.setMessageType(MESSAGE_TYPES[random.nextInt(MESSAGE_TYPES.length)]);

            //时间 咨询->询单（0-5分钟）->下单（2-10分钟）->付款（1-5分钟）
            LocalDateTime consultTime = LocalDateTime.now().minusMinutes(random.nextInt(60 * 24));
            data.setConsultTime(consultTime.format(dtf));

            LocalDateTime inquireTime = consultTime.plusMinutes(random.nextInt(5) + 1);
            data.setInquireTime(inquireTime.format(dtf));

            //30%不下单，50%不付款，20%下单且付款
            if (random.nextDouble() < 0.7) {
                data.setPlaceOrderTime(null);
                data.setPayTime(null);
                data.setPayAmount(0.0);
                data.setPayQuantity(0);
            }else {
                LocalDateTime placeOrderTime = inquireTime.plusMinutes(random.nextInt(10) + 2);
                data.setPlaceOrderTime(placeOrderTime.format(dtf));

                if (random.nextDouble() < 0.3){
                    data.setPayTime(null);
                    data.setPayAmount(0.0);
                    data.setPayQuantity(0);
                }else {
                    LocalDateTime payTime = placeOrderTime.plusMinutes(random.nextInt(5) + 1);
                    data.setPayTime(payTime.format(dtf));
                    data.setPayAmount(50 + random.nextDouble() * 500);  // 付款金额50-550
                    data.setPayQuantity(1 + random.nextInt(10)); // 付款数量1-10
                }
            }
            data.setDimension(DIMNSIONS[random.nextInt(DIMNSIONS.length)]);

            // 发送数据到flink
            String jsonData = JSON.toJSONString(data);
            System.out.println("生成数据：" + jsonData);
            ctx.collect(jsonData);
            currentCount++;
            Thread.sleep(10);


        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ShopPerformanceData {
        private String recorId;
        private String userId;
        private String productId;
        private String productName;
        private String customerServiceId;
        private String csName;
        private String entryChannel;
        private String messageType;
        private String consultTime;
        private String inquireTime;
        private String placeOrderTime;
        private String payTime;
        private Double payAmount;
        private Integer payQuantity;
        private String dimension;
    }
}
