package datagenerator;

import helper.DataGeneratorHelper;
import model.LocationZone;
import model.PaymentData;
import model.Status;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.Date;

public class PaymentDataGenerator implements SourceFunction<PaymentData> {
    public static final int SLEEP_MILLIS_PER_EVENT = 40;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<PaymentData> ctx) throws Exception {
        long var = 1;
        while (running){
            //UUID var = UUID.randomUUID();
            //Long var = new Random().nextLong();
            var += 1;
            Status status = DataGeneratorHelper.getRandomStartStatus();
            LocationZone locationZone = DataGeneratorHelper.getRandomLocationZone();
            PaymentData paymentData = PaymentData.builder()
                    .locationZone(locationZone.name())
                    .componentId(var)
                    .status(status.name())
                    .eventTime(new Date())
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