package org.apache.iotdb.utils.core.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class PipelineBuilder{

    private List<Component> componentChain;

    private SourceBuilder sourceBuilder = new SourceBuilder();
    private ChannelBuilder channelBuilder = new ChannelBuilder();
    private SinkBuilder sinkBuilder = new SinkBuilder();

    public CommonPipeline build(){
        CommonPipeline pipeline = new CommonPipeline();
        pipeline.setComponentChain(componentChain);
        return pipeline;
    }

    public SourceBuilder source(PipeSource source){
        if(componentChain == null){
            componentChain = new ArrayList<>();
        }
        PipeComponent component;
        component = source;
        componentChain.add(component);
        sourceBuilder.wrapper = component;
        return sourceBuilder;
    }

    public SourceBuilder source(Supplier<? extends PipeSource> supplier){
        if(componentChain == null){
            componentChain = new ArrayList<>();
        }
        PipeComponent wrapper;
        wrapper = supplier.get();
        componentChain.add(wrapper);
        sourceBuilder.wrapper = wrapper;
        return sourceBuilder;
    }

    private ChannelBuilder channel(PipeChannel channel, PipeComponent parent){
        PipeComponent wrapper;
        wrapper = channel;
        parent.setNext(wrapper);
        channelBuilder.wrapper = wrapper;
        return channelBuilder;
    }

    private SinkBuilder sink(PipeSink sink, PipeComponent parent){
        Component wrapper;
        wrapper = sink;
        parent.setNext(wrapper);
        return sinkBuilder;
    }

    public class SourceBuilder{
        public PipeComponent wrapper;

        public ChannelBuilder channel(PipeChannel channel){
            return PipelineBuilder.this.channel(channel,wrapper);
        }

        public ChannelBuilder channel(Supplier<? extends PipeChannel> supplier){
            return PipelineBuilder.this.channel(supplier.get(),wrapper);
        }
    }

    public class ChannelBuilder{
        public PipeComponent wrapper;

        public ChannelBuilder channel(PipeChannel channel){
            return PipelineBuilder.this.channel(channel,wrapper);
        }

        public ChannelBuilder channel(Supplier<? extends PipeChannel> supplier){
            return PipelineBuilder.this.channel(supplier.get(),wrapper);
        }

        public SinkBuilder sink(PipeSink sink){
            return PipelineBuilder.this.sink(sink,wrapper);
        }

        public SinkBuilder sink(Supplier<? extends PipeSink> supplier){
            return PipelineBuilder.this.sink(supplier.get(),wrapper);
        }
    }

    public class SinkBuilder{
        public SourceBuilder source(PipeSource source){
            return PipelineBuilder.this.source(source);
        }

        public SourceBuilder source(Supplier<? extends PipeSource> supplier){
            return PipelineBuilder.this.source(supplier.get());
        }

        public CommonPipeline build(){
            return PipelineBuilder.this.build();
        }
    }

}
