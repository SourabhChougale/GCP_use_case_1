import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
public interface Options extends DataflowPipelineOptions {


    @Description("BigQuery table name")
    String getoutputTable();
    void setoutputTableName(String outputTable);

    @Description("Input topic name")
    String getinputTopic();
    void setinputTopic(String inputTopic);


    @Description("The DLQ Topic used to store malformed Messages.")
    String getdlqTopic();
    void setdlqTopic(String dlqTopic);
}
