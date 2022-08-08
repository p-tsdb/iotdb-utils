package org.apache.iotdb.utils.core.pipeline.context;

import org.apache.iotdb.utils.core.pipeline.context.model.PipelineModel;
import lombok.Data;

@Data
public class PipelineContext<R extends PipelineModel> {

    private String name = "pipeline-context";

    private R model;

}
