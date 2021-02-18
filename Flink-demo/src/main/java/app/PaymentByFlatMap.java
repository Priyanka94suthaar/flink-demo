package app;

import datagenerator.PaymentDataGenerator;
import model.PaymentData;
import model.Status;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class PaymentByFlatMap {
    public static void main(String[] args) throws Exception {


        if (args.length == 2){

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);
            DataStream<PaymentData> payments = env.addSource(new PaymentDataGenerator());
            //payments.writeAsText("C:\\Users\\Priyanka\\Downloads\\Input.txt");
            DataStream<PaymentData> paymentFiltered  = payments.filter(new FilterFunction<PaymentData>() {
                @Override
                public boolean filter(PaymentData value) throws Exception {
                    return value.getStatus().equals(Status.STARTED.name());
                }
            }).keyBy(PaymentData::getComponentId);
           //paymentFiltered.print();
            DataStream<Tuple2<String, Integer>> counts = paymentFiltered
                    .map(new LocationZone())
                    .keyBy(0)
                    .sum(1);
            //  .flatMap(new TokenizerClass()).keyBy(value->value.f0.getLocationZone()).sum(1);
            //counts.print();

            paymentFiltered.writeAsText(args[0]);
            counts.writeAsText(args[1]);

            env.execute();
        }else{
            // close app
        }



    }
}

final class LocationZone implements MapFunction<PaymentData, Tuple2<String, Integer>>{

    @Override
    public Tuple2<String, Integer> map(PaymentData paymentData) throws Exception {
        return new Tuple2<String, Integer>(paymentData.getLocationZone(), 1);
    }
}

final class TokenizerClass
        implements FlatMapFunction<PaymentData, Tuple2<PaymentData, Integer>> {

    @Override
    public void flatMap(PaymentData value, Collector<Tuple2<PaymentData, Integer>> out) {

                out.collect(new Tuple2<>(value, 1));

          }
}
