import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
public interface Options extends DataflowPipelineOptions {


    @Description("BigQuery table name")
    String getOutputTableName();
    void setOutputTableName(String outputTable);

    @Description("Input topic name")
    String getInputTopic();
    void setInputTopic(String inputTopic);


    @Description("The DLQ Topic used to store malformed Messages.")
    String getDLQTopic();
    void setDLQTopic(String dlqTopic);
}