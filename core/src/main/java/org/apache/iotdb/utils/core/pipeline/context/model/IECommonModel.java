package org.apache.iotdb.utils.core.pipeline.context.model;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.Data;
import org.apache.iotdb.session.Session;
import reactor.core.publisher.SignalType;

import java.util.function.Consumer;

@Data
public class IECommonModel extends PipelineModel{

    @JSONField(serialize = false)
    private Session session;

    @JSONField(serialzeFeatures = {SerializerFeature.WriteEnumUsingToString})
    private CompressEnum compressEnum;

    private String charSet;

    private String fileFolder;

    @JSONField(serialzeFeatures = {SerializerFeature.WriteEnumUsingToString})
    private FileSinkStrategyEnum fileSinkStrategyEnum;

    private Boolean needTimeseriesStructure;

    @JSONField(serialize = false)
    @Deprecated
    private Boolean zipCompress;

    @JSONField(serialize = false)
    private int parallelism;

    //回调方法，pipeline运行完毕后悔调用此方法
    @JSONField(serialize = false)
    private Consumer<SignalType> consumer;
}
