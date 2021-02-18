package datagenerator;

import helper.DataGeneratorHelper;
import model.LocationZone;
import model.PaymentData;
import model.PaymentDataCase2;
import model.Status;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.Date;

public class CompletionDataGenerator implements SourceFunction<PaymentDataCase2>{

    public static final int SLEEP_MILLIS_PER_EVENT = 40;
    private volatile boolean running = true;

    @Override
    public void run(SourceFunction.SourceContext<PaymentDataCase2> ctx) throws Exception {
        long var = 1;
        while (running){
            //UUID var = UUID.randomUUID();
            //Long var = new Random().nextLong();
            var = (long)DataGeneratorHelper.getRandomCompId();;
            Status status = DataGeneratorHelper.getRandomCompletionStatus();
            LocationZone locationZone = DataGeneratorHelper.getRandomLocationZone();
            PaymentDataCase2 paymentData = PaymentDataCase2.builder()
                    .locationZone(locationZone.name())
                    .componentId(var)
                    .status(status.name())
                    .eventTime(new Date().getTime())
                    .build();
            ctx.collectWithTimestamp(paymentData, Instant.now().toEpochMilli());
            Thread.sleep(SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }

}
