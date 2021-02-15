package app;

import datagenerator.PaymentDataGenerator;
import model.LocationZone;
import model.PaymentData;
import model.Status;
import model.ZoneEventsCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

public class PaymentByWindow {

    private static Map<String, Integer> zoneToCountMap = Maps.newHashMap();
    static {
        zoneToCountMap.put(LocationZone.EAST.name(), 0);
        zoneToCountMap.put(LocationZone.WEST.name(), 0);
        zoneToCountMap.put(LocationZone.NORTH.name(), 0);
        zoneToCountMap.put(LocationZone.SOUTH.name(), 0);
    }
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<PaymentData> payments = env.addSource(new PaymentDataGenerator());
        //payments.writeAsText("C:\\Users\\Priyanka\\Downloads\\Input.txt");
        DataStream<PaymentData> paymentFiltered  = payments.filter(new FilterFunction<PaymentData>() {
            @Override
            public boolean filter(PaymentData value) throws Exception {
                return value.getStatus().equals(Status.STARTED.name());
            }
        })/*.keyBy(PaymentData::getComponentId)*/;
        paymentFiltered.print();
        paymentFiltered.timeWindowAll(Time.seconds(4)).process(new ProcessAllWindowFunction<PaymentData, ZoneEventsCount, TimeWindow>() {

            @Override
            public void process(Context context, Iterable<PaymentData> iterable, Collector<ZoneEventsCount> collector) throws Exception {

                iterable.forEach(paymentData -> {
                    zoneToCountMap.put(paymentData.getLocationZone(),
                            zoneToCountMap.get(paymentData.getLocationZone()) + 1);
                });

                int totalCount = zoneToCountMap.values().stream().reduce(Integer::sum).get();
                ZoneEventsCount ze = ZoneEventsCount.builder()
                        .eastZoneCount(zoneToCountMap.get(LocationZone.EAST.name()))
                        .westZoneCount(zoneToCountMap.get(LocationZone.WEST.name()))
                        .northZoneCount(zoneToCountMap.get(LocationZone.NORTH.name()))
                        .southZoneCount(zoneToCountMap.get(LocationZone.SOUTH.name()))
                        .totalCount(totalCount)
                        .build();
                collector.collect(ze);
            }
        }).print();
        env.execute();
    }
}

