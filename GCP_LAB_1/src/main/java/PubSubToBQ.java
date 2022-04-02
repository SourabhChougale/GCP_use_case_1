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
import com.google.api.services.bigquery.model.TableSchema;
import com.google.google.storage.v1.Object;
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
import org.apache.beam.sdk.options.ValueProvider;
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


public class PubSubToBQ {
    static final TupleTag<String> parsedMessages = new TupleTag<String>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };

    private static final Logger LOG = LoggerFactory.getLogger(Demo.class);


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



    @DefaultSchema(JavaFieldSchema.class)
    public static class PubsubMessageToRow extends PTransform<PCollection<String>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            return input
                    .apply("JsonToRow", ParDo.of(new DoFn<String, String>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    String json = context.element();
                                    JSONObject obj=new JSONObject(json);
                                    try{
                                        if(rawSchema.getFieldCount()==3)
                                        {
                                        if (obj.get("id") instanceof Integer && obj.get("name") instanceof String && obj.get("surname") instanceof String)
                                            context.output(parsedMessages, json);
                                    }
                                    else{
                                        context.output(unparsedMessages, json);
                                    }
                                    }catch (Exception e) {
                                        context.output(unparsedMessages, json);
                                    }




                                }
                            })
                            .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


        }
    }

    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();




    public static PipelineResult run(Option options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);




        /*
         * Steps:
         * 1) Read something
         * 2) Transform something
         * 3) Write something
         */

        PCollectionTuple  pubsub=pipeline.apply("ReadFromGCS", PubsubIO.readStrings().fromTopic(options.getinputTopic()))
                .apply("Message parsing", new PubsubMessageToRow());
        
        pubsub.get(parsedMessages)
                .apply("To row",JsonToRow.withSchema(rawSchema))
                .apply("WriteToBQ", BigQueryIO.<Row>write().to(options.getoutputTable())
                        .useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pubsub.get(unparsedMessages)
                .apply("WriteToDLQ",PubsubIO.writeStrings().to(options.getdlqTopic()));


        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}
