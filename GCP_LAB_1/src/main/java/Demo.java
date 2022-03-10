/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Demo {
    static final TupleTag<TableRow> parsedMessages = new TupleTag<TableRow>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };
    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Demo.class);

    /**
     * The {@link Option} class provides the custom execution options passed by the
     * executor at the command-line.
     */
    public interface Option extends DataflowPipelineOptions {
        @Description("BigQuery table name")
        String getoutputTable();
        void setoutputTable(String outputTable);

        @Description("Input topic name")
        String getinputTopic();
        void setinputTopic(String inputTopic);


        @Description("The DLQ Topic used to store malformed Messages.")
        String getdlqTopic();
        void setdlqTopic(String dlqTopic);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the {@link Demo#run(Option)} method to
     * start the pipeline and invoke {@code result.waitUntilFinish()} on the
     * {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Option options = PipelineOptionsFactory.fromArgs(args).as(Option.class);

        run(options);
    }

    /**
     * A class used for parsing JSON web server events
     * Annotated with @DefaultSchema to the allow the use of Beam Schemas and <Row> object
     */
   /* @DefaultSchema(JavaFieldSchema.class)
    public static class CommonLog {
        int id;
        String name;
        String surname;

    }*/

    /**
     * A DoFn acccepting Json and outputing CommonLog with Beam Schema
     */
 /*   static class JsonToCommonLog extends DoFn<String, CommonLog> {

       @ProcessElement
       public void processElement(@Element String json, OutputReceiver<CommonLog> r) throws JsonSyntaxException {
            try {
               // Gson gson = new Gson();
                CommonLog commonLog = new Gson().fromJson(json, CommonLog.class);
                r.output(commonLog);
            }catch(JsonSyntaxException e){
                e.printStackTrace();
            }


        }
    }*/
    public static class PubsubMessageToRow extends PTransform<PCollection<String>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            return input
                    .apply("JsonToRow", ParDo.of(new DoFn<String, TableRow>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    String json = context.element();
                                    JSONObject obj=new JSONObject(json);

                                    try {
                                        TableRow row=new TableRow().
                                                set("id",obj.getInt("id")).
                                                set("name",obj.getString("name")).
                                                set("surname",obj.getString("surname"));
                                        context.output(parsedMessages,row);
                                    } catch (JsonSyntaxException e) {
                                        context.output(unparsedMessages, json);
                                    }

                                }
                            })
                            .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


        }
    }
    /**
     * Runs the pipeline to completion with the specified options. This method does
     * not wait until the pipeline is finished before returning. Invoke
     * {@code result.waitUntilFinish()} on the result object to block until the
     * pipeline is finished running if blocking programmatic execution is required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
 /*   public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();
*/

    public static PipelineResult run(Option options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);


    // Static input and output
    String input = options.getinputTopic();
    String output = options.getoutputTable();

    /*
     * Steps:
     * 1) Read something
     * 2) Transform something
     * 3) Write something
     */

        PCollectionTuple  pubsub=pipeline.apply("ReadFromGCS", PubsubIO.readStrings().fromTopic(input))
                .apply("ConvertMessageToCommonLog", new PubsubMessageToRow());



       pubsub.get(parsedMessages)
               .apply("WriteToBQ", BigQueryIO.<TableRow>write().to(output).useBeamSchema()
                       .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                       .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

       pubsub.get(unparsedMessages)
               .apply("WriteToDLQ",PubsubIO.writeStrings().to(options.getdlqTopic()));


        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}
