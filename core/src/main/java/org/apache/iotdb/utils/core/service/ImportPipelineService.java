package org.apache.iotdb.utils.core.service;

import org.apache.iotdb.utils.core.pipeline.context.PipelineContext;
import org.apache.iotdb.utils.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.utils.core.utils.IotDBKeyWords;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.utils.Binary;
import reactor.core.publisher.Flux;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.*;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

/**
 * @Author: LL
 * @Description:
 * @Date: create in 2022/7/1 9:41
 */
@Slf4j
public class ImportPipelineService {

    private static ImportPipelineService importPipelineService;

    private ImportPipelineService() {

    }

    public static ImportPipelineService importPipelineService() {
        if (importPipelineService == null) {
            importPipelineService = new ImportPipelineService();
        }
        return importPipelineService;
    }

    /**
     * 获取文件名的flux
     * filter  排除不需要的文件
     *
     * @return
     */
    public Flux<InputStream> parseFluxFileName(FilenameFilter filter,ConcurrentHashMap<String, List<InputStream>> COMPRESS_MAP) {
        return Flux.deferContextual(contextView -> {
            PipelineContext<ImportModel> context = contextView.get("pipelineContext");
            ImportModel importModel = context.getModel();
            String dic = importModel.getFileFolder();
            File[] fileArray = this.getFileArray(filter,dic);
            List<InputStream> inputStreamList = new ArrayList<>();
            try {
                for (int i = 0; i < fileArray.length; i++) {
                    File file = fileArray[i];
                    InputStream in = new FileInputStream(file);
                    inputStreamList.add(in);
                }
            } catch (IOException e) {
                log.error("异常信息:",e);
            }
            COMPRESS_MAP.put("inputStreamList",inputStreamList);
            return Flux.fromIterable(inputStreamList);
        });
    }

    public File[] getFileArray(FilenameFilter filter,String dic){
        File fileDic = new File(dic);
        if (!fileDic.exists()) {
            throw new IllegalArgumentException("file folder does not exists");
        }
        if (!fileDic.isDirectory()) {
            throw new IllegalArgumentException("file folder is not a dictionary");
        }

        File[] fileArray = fileDic.listFiles(filter);
        return fileArray;
    }

    public static String formatPath(String sql){
        String regx = "^[\\w._:@#{}$\\u2E80-\\u9FFF\"\' *\\\\]+$";
        String regxOnlyNum = "^[0-9]+$";
        char[] arr = sql.toCharArray();
        StringBuilder formatedSql = new StringBuilder();
        StringBuilder buffer = new StringBuilder();
        StringBuilder quoteCounter = new StringBuilder();
        for(int i = 0; i< arr.length; i++){
            if('\\' == arr[i] || '\"' == arr[i] || '\'' == arr[i]){
                if(quoteCounter.length() == 0){
                    quoteCounter.append(arr[i]);
                }else {
                    char quote = quoteCounter.charAt(quoteCounter.length()-1);
                    if("\\".equals(quote)){
                        quoteCounter.deleteCharAt(quoteCounter.length()-1);
                    }
                }
            }else{
                if(quoteCounter.length()!=0){
                    quoteCounter.delete(0,quoteCounter.length());
                }
            }

            if(',' == arr[i] || i == arr.length-1){
                if(buffer.length() != 0){
                    // quote 引用处理
                    if('\"' ==buffer.charAt(0) || '\'' == buffer.charAt(0)){
                        char pre = buffer.charAt(buffer.length()-1);
                        buffer.append(arr[i]);
                        if(pre == buffer.charAt(0) && quoteCounter.length() == 0){
                            formatedSql.append(buffer);
                            buffer.delete(0,buffer.length());
                        }
                    }else{
                        //特殊字符处理  iotdb 系统关键字处理
                        if(i == arr.length-1){
                            buffer.append(arr[i]);
                        }
                        if(!Pattern.matches(regx,buffer.toString()) || IotDBKeyWords.validateKeyWords(buffer.toString().toUpperCase()) || Pattern.matches(regxOnlyNum,buffer.toString())){
                            if(i == arr.length -1){
                                if(buffer.toString().startsWith("`") && buffer.toString().endsWith("`")){
                                    formatedSql.append(buffer);
                                }else{
                                    formatedSql.append("`").append(buffer).append("`");
                                }
                            }else{
                                if(buffer.toString().startsWith("`") && buffer.toString().endsWith("`")){
                                    formatedSql.append(buffer).append(",");
                                }else{
                                    formatedSql.append("`").append(buffer).append("`").append(",");
                                }
                            }
                        }else {
                            if(i != arr.length-1){
                                buffer.append(arr[i]);
                            }
                            formatedSql.append(buffer);
                        }

                        buffer.delete(0,buffer.length());
                    }
                }else{
                    if(!Pattern.matches(regx,buffer.toString()) || IotDBKeyWords.validateKeyWords(buffer.toString().toUpperCase())){
                        buffer.append("`").append(arr[i]).append("`");
                    }else if(Pattern.matches(regxOnlyNum,buffer.toString())){
                        buffer.append("`").append(arr[i]).append("`");
                    }else {
                        buffer.append(arr[i]);
                    }
                }
            }else{
                buffer.append(arr[i]);
            }
        }
        formatedSql.append(buffer);
        return formatedSql.toString();
    }

    public Field generateFieldValue(Field field, String s) {
        if(s == null || "".equals(s)){
            return null;
        }
        switch (field.getDataType()){
            case TEXT:
                if (s.startsWith("\"") && s.endsWith("\"")) {
                    s = s.substring(1, s.length() - 1);
                }
                field.setBinaryV(Binary.valueOf(s));
                break;
            case BOOLEAN:
                field.setBoolV(Boolean.parseBoolean(s));
                break;
            case INT32:
                field.setIntV(Integer.parseInt(s));
                break;
            case INT64:
                field.setLongV(Long.parseLong(s));
                break;
            case FLOAT:
                field.setFloatV(Float.parseFloat(s));
                break;
            case DOUBLE:
                field.setDoubleV(Double.parseDouble(s));
                break;
            default:
                throw new IllegalArgumentException(": not support type,can not convert to TSDataType");
        }
        return field;
    }

    public TSDataType parseTsDataType(String type) {
        switch (type) {
            case "TEXT":
                return TEXT;
            case "BOOLEAN":
                return BOOLEAN;
            case "INT32":
                return INT32;
            case "INT64":
                return INT64;
            case "FLOAT":
                return FLOAT;
            case "DOUBLE":
                return DOUBLE;
            default:
                throw new IllegalArgumentException(type + ": not support type,can not convert to TSDataType");
        }
    }
}
