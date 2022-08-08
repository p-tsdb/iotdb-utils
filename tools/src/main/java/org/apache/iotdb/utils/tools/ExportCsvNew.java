/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.utils.tools;

import org.apache.iotdb.utils.core.exception.ParamCheckException;
import org.apache.iotdb.utils.core.model.CompressType;
import org.apache.iotdb.utils.core.service.ExportCsvService;
import org.apache.iotdb.utils.core.model.ExportModel;
import org.apache.iotdb.utils.tools.Exception.ArgsErrorException;
import org.apache.iotdb.utils.tools.utils.AbstractCsvTool;
import org.apache.iotdb.utils.tools.utils.JlineUtils;
import org.apache.commons.cli.*;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.thrift.TException;
import org.jline.reader.LineReader;

import java.io.*;

/**
 * Export CSV file.
 *
 * @version 1.0.0 20170719
 */
public class ExportCsvNew extends AbstractCsvTool {

    private static final String TARGET_DIR_ARGS = "td";
    private static final String TARGET_DIR_NAME = "targetDirectory";

    private static final String TARGET_FILE_ARGS = "f";
    private static final String TARGET_FILE_NAME = "targetFile";

    private static final String SQL_FILE_ARGS = "s";
    private static final String SQL_FILE_NAME = "sqlfile";

    private static final String DATA_TYPE_ARGS = "datatype";
    private static final String DATA_TYPE_NAME = "datatype";

    private static final String QUERY_COMMAND_ARGS = "q";
    private static final String QUERY_COMMAND_NAME = "queryCommand";

    protected static final String APPEND_ARGS = "ap";
    protected static final String APPEND_NAME = "append";

    private static final String TSFILEDB_CLI_PREFIX = "ExportCsv";

    private static final String DUMP_FILE_NAME_DEFAULT = "dump";
    private static String targetFile = DUMP_FILE_NAME_DEFAULT;

    private static String targetDirectory;

    private static Boolean needDataTypePrinted;

    private static String queryCommand;

    private static Boolean append;

    /**
     * main function of export csv tool.
     */
    public static void main(String[] args) {
        CommandLine commandLine = null;
        CommandLineParser parser = new DefaultParser();
        int exitCode = CODE_OK;

        commandLine = init(args, commandLine, parser);
        try {
            initSession();
            ExportCsvService exportCsvService = new ExportCsvService();
            ExportModel exportModel = generateExportModel();
            if (queryCommand == null) {
                String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
                String sql;

                if (sqlFile == null) {
                    LineReader lineReader = JlineUtils.getLineReader(username, host, port);
                    sql = lineReader.readLine(TSFILEDB_CLI_PREFIX + "> please input query: ");
                    System.out.println(sql);
                    String[] values = sql.trim().split(";");
                    for (int i = 0; i < values.length; i++) {
                        exportCsvService.dumpResult(values[i], i,session,exportModel);
                    }
                } else {
                    exportCsvService.dumpFromSqlFile(sqlFile,session,exportModel);
                }
            } else {
                exportCsvService.dumpResult(queryCommand, 0,session,exportModel);
            }

        } catch (IOException e) {
            System.out.println("Failed to operate on file, because " + e.getMessage());
            exitCode = CODE_ERROR;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            System.out.println("Connect failed because " + e.getMessage());
            exitCode = CODE_ERROR;
        } catch (TException e) {
            System.out.println("Cannot dump result because: " + e.getMessage());
            exitCode = CODE_ERROR;
        } catch (ParamCheckException e) {
            System.out.println("Cannot dump result because: " + e.getMessage());
            exitCode = CODE_ERROR;
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (IoTDBConnectionException e) {
                    exitCode = CODE_ERROR;
                    System.out.println(
                            "Encounter an error when closing session, error is: " + e.getMessage());
                }
            }
        }
        System.exit(exitCode);
    }

    private static ExportModel generateExportModel(){
        ExportModel exportModel = new ExportModel();
        exportModel.setNeedDataTypePrinted(needDataTypePrinted);
        exportModel.setTargetDirectory(targetDirectory);
        exportModel.setTargetFile(targetFile);
        exportModel.setTimeFormat(timeFormat);
        exportModel.setCompressType(compressType);
        exportModel.setZoneId(zoneId);
        exportModel.setCharSet(charSet);
        exportModel.setAppend(append);
        return exportModel;
    }

    private static CommandLine init(String[] args, CommandLine commandLine, CommandLineParser parser) {
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(null); // avoid reordering
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

        if (args == null || args.length == 0) {
            System.out.println("Too few params input, please check the following hint.");
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            System.exit(CODE_ERROR);
        }
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            System.exit(CODE_ERROR);
        }
        if (commandLine.hasOption(HELP_ARGS)) {
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            System.exit(CODE_ERROR);
        }
        try {
            parseBasicParams(commandLine);
            parseSpecialParams(commandLine);
        } catch (ArgsErrorException e) {
            System.out.println("Args error: " + e.getMessage());
            System.exit(CODE_ERROR);
        } catch (Exception e1) {
            System.out.println("Encounter an error, because: " + e1.getMessage());
            System.exit(CODE_ERROR);
        }
        if (!checkTimeFormat()) {
            System.exit(CODE_ERROR);
        }
        return commandLine;
    }

    private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException, ParamCheckException {
        targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine);
        targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
        needDataTypePrinted = Boolean.valueOf(commandLine.getOptionValue(DATA_TYPE_ARGS));
        queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);

        if (commandLine.getOptionValue(DATA_TYPE_ARGS) == null
                || commandLine.getOptionValue(DATA_TYPE_ARGS).isEmpty()) {
            needDataTypePrinted = true;
        }

        timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
        if (timeFormat == null) {
            timeFormat = "default";
        }
        timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
        if (!targetDirectory.endsWith("/") && !targetDirectory.endsWith("\\")) {
            targetDirectory += File.separator;
        }

        String compressAlgorithm = commandLine.getOptionValue(COMPRESS_ARGS);
        if ("snappy".equals(compressAlgorithm)) {
            compressType = CompressType.SNAPPY;
        } else if("gzip".equals(compressAlgorithm)){
            compressType = CompressType.GZIP;
        }else{
            compressType = CompressType.NONE;
        }

        String charSetAlgorithm = commandLine.getOptionValue(CHAR_SET_ARGS);
        if(charSetAlgorithm == null || "".equals(charSetAlgorithm)){
            charSet = "utf8";
        }else{
            charSet = charSetAlgorithm;
        }

        String appendAlgorithm = commandLine.getOptionValue(APPEND_ARGS);
        if(appendAlgorithm == null || "".equals(appendAlgorithm)){
            append = false;
        }else{
            append = Boolean.parseBoolean(appendAlgorithm);
        }
        if(append && targetFile == null){
            throw new ParamCheckException("the param append is true,a target file is needed");
        }
        if (targetFile == null) {
            targetFile = DUMP_FILE_NAME_DEFAULT;
        }
    }

    /**
     * commandline option create.
     *
     * @return object Options
     */
    private static Options createOptions() {

        Options options = createNewOptions();

        Option opTargetFile =
                Option.builder(TARGET_DIR_ARGS)
                        .required()
                        .argName(TARGET_DIR_NAME)
                        .hasArg()
                        .desc("Target File Directory (required)")
                        .build();
        options.addOption(opTargetFile);

        Option targetFileName =
                Option.builder(TARGET_FILE_ARGS)
                        .argName(TARGET_FILE_NAME)
                        .hasArg()
                        .desc("Export file name (optional)")
                        .build();
        options.addOption(targetFileName);

        Option opSqlFile =
                Option.builder(SQL_FILE_ARGS)
                        .argName(SQL_FILE_NAME)
                        .hasArg()
                        .desc("SQL File Path (optional)")
                        .build();
        options.addOption(opSqlFile);

        Option opTimeFormat =
                Option.builder(TIME_FORMAT_ARGS)
                        .argName(TIME_FORMAT_NAME)
                        .hasArg()
                        .desc(
                                "Output time Format in csv file. "
                                        + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
                                        + "user-defined pattern like yyyy-MM-dd\\ HH:mm:ss, default ISO8601 (optional)")
                        .build();
        options.addOption(opTimeFormat);

        Option opTimeZone =
                Option.builder(TIME_ZONE_ARGS)
                        .argName(TIME_ZONE_NAME)
                        .hasArg()
                        .desc("Time Zone eg. +08:00 or -01:00 (optional)")
                        .build();
        options.addOption(opTimeZone);

        Option opDataType =
                Option.builder(DATA_TYPE_ARGS)
                        .argName(DATA_TYPE_NAME)
                        .hasArg()
                        .desc(
                                "Will the data type of timeseries be printed in the head line of the CSV file?"
                                        + '\n'
                                        + "You can choose true) or false) . (optional)")
                        .build();
        options.addOption(opDataType);

        Option opQuery =
                Option.builder(QUERY_COMMAND_ARGS)
                        .argName(QUERY_COMMAND_NAME)
                        .hasArg()
                        .desc("The query command that you want to execute. (optional)")
                        .build();
        options.addOption(opQuery);

        Option opHelp =
                Option.builder(HELP_ARGS)
                        .longOpt(HELP_ARGS)
                        .hasArg(false)
                        .desc("Display help information")
                        .build();
        options.addOption(opHelp);

        Option opCompress =
                Option.builder(COMPRESS_ARGS)
                        .longOpt(COMPRESS_NAME)
                        .argName(COMPRESS_NAME)
                        .hasArg()
                        .desc("Type snappy/gzip to use compress algorithm. (optional)")
                        .build();
        options.addOption(opCompress);

        Option charSet =
                Option.builder(CHAR_SET_ARGS)
                        .argName(CHAR_SET_NAME)
                        .hasArg()
                        .desc("the file`s charset,default utf8.(optional)")
                        .build();
        options.addOption(charSet);

        Option append =
                Option.builder(APPEND_ARGS)
                        .argName(APPEND_NAME)
                        .hasArg()
                        .desc("the file`s append model,the new dataset will append to the file.(optional)")
                        .build();
        options.addOption(append);

        return options;
    }
}
