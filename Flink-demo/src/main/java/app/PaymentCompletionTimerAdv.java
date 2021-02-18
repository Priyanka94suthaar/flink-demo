package app;

import datagenerator.CompletionDataGenerator;
import model.PaymentData;
import model.PaymentDataCase2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PaymentCompletionTimerAdv {
    public static void main(String[] args) throws Exception {


        if (args.length == 2){
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(4);
            DataStream<PaymentDataCase2> payments = env.addSource(new CompletionDataGenerator());

            DataStream<Tuple2<String, Long>> completionInterval = payments
                    .keyBy(event -> event.getComponentId())
                    .process(new IntervalProcess());
            DataStream<Tuple2<String, Long>> finalInterval =
                    completionInterval.filter(new FilterFunction<Tuple2<String, Long>>() {
                        @Override
                        public boolean filter(Tuple2<String, Long> value) throws Exception {
                            if("No-Alerts".equals(value.f0)){
                                return false;
                            }else{
                                System.out.println("\n!! Match Alert Received : Payment Id "
                                        + value.f0 + " Time to Complete is "
                                        + value.f1 + " ms" + "\n");
                                return true;
                            }
                        }
                    });

            payments.writeAsText(args[0]);
            finalInterval.writeAsText(args[1]);
            env.execute();
        }

    }
}




