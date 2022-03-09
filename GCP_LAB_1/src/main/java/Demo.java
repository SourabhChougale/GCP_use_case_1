import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
public class Demo {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBQ.class);


    static class JsonToCommonLog extends DoFn<String, Account> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<Account> r) throws Exception {
            Gson gson = new Gson();
            Account a = gson.fromJson(json, Account.class);
            r.output(a);
        }
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        run(options);
    }

    public static PipelineResult run(Options options) {
    Pipeline pipeline=Pipeline.create(options);

        pipeline.apply("ReadFromTopic", PubsubIO.readStrings().fromTopic(options.getinputTopic()))
                .apply("ParseJson", ParDo.of(new JsonToCommonLog()))
                .apply("WriteToBQ",
                        BigQueryIO.<Account>write().to(options.getoutputTable()).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    LOG.info("Building pipeline...");
    return pipeline.run();
    }
}
