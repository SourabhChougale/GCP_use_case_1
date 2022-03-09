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

public class PubSubToBQ {
    static final TupleTag<Account> parsedMessages = new TupleTag<Account>() {};
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {};

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBQ.class);


    static class parsing extends DoFn<String,Account> {
        @ProcessElement
        public void processElement(@Element String json,ProcessContext processContext)throws Exception{
            try {
                Gson gson = new Gson();
                Account account = gson.fromJson(json, Account.class);
                processContext.output(parsedMessages,account);
            }catch(Exception e){
                e.printStackTrace();
                processContext.output(unparsedMessages,json);
            }
        }
    }



    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        run(options);
    }

    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        LOG.info("Building pipeline...");


        PCollectionTuple input =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings().fromTopic(options.getinputTopic()))
                        .apply("MessageParsing", ParDo.of(new parsing()).withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));

        PCollection<Account> validData = input.get(parsedMessages);
        PCollection<String> invalidData = input.get(unparsedMessages);

                       /* .apply("GsontoJson",ParDo.of(new DoFn<Account, String>() {
                            @ProcessElement
                            public void convert(ProcessContext context){
                                Gson g = new Gson();
                                String gsonString = g.toJson(context.element());
                                context.output(gsonString);
                            }
                        }))
                        .apply("Json To Row Convertor",JsonToRow.withSchema(rawSchema))*/
               validData .apply("WriteToBQ", BigQueryIO.<Account>write().to(options.getoutputTable())
                        .useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        // Write unparsed messages to Cloud Storage

                // Retrieve unparsed messages
        invalidData
                .apply("WriteInDLQtopic", TextIO.write().to(options.getdlqTopic()));
                

        return pipeline.run();

    }
}
