import java.io.IOException;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.dnanexus.DXAPI;
import com.dnanexus.DXJSON;
import com.dnanexus.DXUtil;
import com.dnanexus.DXWorkflow;
import com.dnanexus.DXWorkflow.Describe;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Code {

    // This is an example for how to get workflow information with the Java DXAPI.
    // It is handy to consult the description of the workflow-describe API, at:
    // https://wiki.dnanexus.com/API-Specification-v1.0.0/Workflows-and-Analyses#API-method%3A-%2Fworkflow-xxxx%2Fdescribe

    // Usage example from the command line:
    //   java -classpath .:lib/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar  Code "workflow-F8jy26804YPjf64kBx12Z7Fv"
    //

    private static class ReqInput {
        @JsonProperty
        public Map<String, Boolean> fields = new TreeMap<String,Boolean>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ReqOutput {
        @JsonProperty
        public List<Map<String, String>> inputSpec;

        @JsonProperty
        public List<Map<String, String>> outputSpec;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: <workflow>");
            System.exit(1);
        }
        String workflowID = args[0];

        // Nice and pleasant way, using the java SDK
        DXWorkflow dxWf = DXWorkflow.getInstance(workflowID);
        DXWorkflow.Describe desc = dxWf.describe();
        System.err.println("project=" + desc.getProject());
        System.err.println("name=" + desc.getName());
        System.err.println("\n");

        // Hand crafted request, for additional fields
        ReqInput req = new ReqInput();
        req.fields.put("inputSpec", true);
        req.fields.put("outputSpec", true);

        //ReqOutput output = DXAPI.workflowDescribe(dxWf.getId(), req, ReqOutput.class);

        ReqOutput output = DXJSON.safeTreeToValue(
            apiCallOnObject("describe", MAPPER.valueToTree(options),
                            RetryStrategy.SAFE_TO_RETRY),
            ReqOutput.class);

    }
}
