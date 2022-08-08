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
import org.apache.iotdb.utils.core.service.ImportCsvService;
import org.apache.iotdb.utils.core.model.ImportModel;
import org.apache.iotdb.utils.tools.Exception.ArgsErrorException;
import org.apache.iotdb.utils.tools.utils.AbstractCsvTool;
import org.apache.commons.cli.*;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.io.File;
import java.io.IOException;

public class ImportCsvNew extends AbstractCsvTool {

    private static final String FILE_ARGS = "f";
    private static final String FILE_NAME = "file or folder";

    private static final String FAILED_FILE_ARGS = "fd";
    private static final String FAILED_FILE_NAME = "failed file directory";

    private static final String BATCH_POINT_SIZE_ARGS = "batch";
    private static final String BATCH_POINT_SIZE_NAME = "batch point size";

    private static final String ALIGNED_ARGS = "aligned";
    private static final String ALIGNED_NAME = "use the aligned interface";

    private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";

    private static String targetPath;
    private static String failedFileDirectory = null;
    private static Boolean aligned = false;

    private static int batchPointSize = 100_000;

    /**
     *
     * @param args
     * @throws IoTDBConnectionException
     */
    public static void main(String[] args){
        init(args);
        int exitCode = CODE_OK;
        try{
            initSession();
            ImportCsvService importCsvService = new ImportCsvService();
            ImportModel importModel = generateImportCsvFile();
            importCsvService.importFromTargetPath(session, importModel);
        } catch (IOException e) {
            System.out.println("Failed to operate on file, because " + e.getMessage());
            exitCode = CODE_ERROR;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            System.out.println("Connect failed because " + e.getMessage());
            exitCode = CODE_ERROR;
        }catch (ParamCheckException e) {
            System.out.println(
                    "param check failed, because: " + e.getMessage());
            exitCode = CODE_ERROR;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            exitCode = CODE_ERROR;
        } finally {
            if(session != null){
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

    /**
     * params init,like host port etc.
     * @param args
     */
    public static void init(String[] args){
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(null);
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        CommandLine commandLine = null;
        CommandLineParser parser = new DefaultParser();

        if (args == null || args.length == 0) {
            System.out.println("Too few params input, please check the following hint.");
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            System.exit(CODE_ERROR);
        }
        try {
            commandLine = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println("Parse error: " + e.getMessage());
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            System.exit(CODE_ERROR);
        }
        if (commandLine.hasOption(HELP_ARGS)) {
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            System.exit(CODE_ERROR);
        }

        try {
            parseBasicParams(commandLine);
            String filename = commandLine.getOptionValue(FILE_ARGS);
            if (filename == null) {
                hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
                System.exit(CODE_ERROR);
            }
            parseSpecialParams(commandLine);
        } catch (ArgsErrorException e) {
            System.out.println("Args error: " + e.getMessage());
            System.exit(CODE_ERROR);
        } catch (Exception e) {
            System.out.println("Encounter an error, because: " + e.getMessage());
            System.exit(CODE_ERROR);
        }
    }

    /**
     * create the commandline options.
     *
     * @return object Options
     */
    private static Options createOptions() {
        Options options = createNewOptions();

        Option opFile =
                Option.builder(FILE_ARGS)
                        .required()
                        .argName(FILE_NAME)
                        .hasArg()
                        .desc(
                                "If input a file path, load a csv file, "
                                        + "otherwise load all csv file under this directory (required)")
                        .build();
        options.addOption(opFile);

        Option opFailedFile =
                Option.builder(FAILED_FILE_ARGS)
                        .argName(FAILED_FILE_NAME)
                        .hasArg()
                        .desc(
                                "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)")
                        .build();
        options.addOption(opFailedFile);

        Option opAligned =
                Option.builder(ALIGNED_ARGS)
                        .argName(ALIGNED_NAME)
                        .hasArg()
                        .desc("Whether to use the interface of aligned (optional)")
                        .build();
        options.addOption(opAligned);

        Option opHelp =
                Option.builder(HELP_ARGS)
                        .longOpt(HELP_ARGS)
                        .hasArg(false)
                        .desc("Display help information")
                        .build();
        options.addOption(opHelp);

        Option opTimeZone =
                Option.builder(TIME_ZONE_ARGS)
                        .argName(TIME_ZONE_NAME)
                        .hasArg()
                        .desc("Time Zone eg. +08:00 or -01:00 (optional)")
                        .build();
        options.addOption(opTimeZone);

        Option opBatchPointSize =
                Option.builder(BATCH_POINT_SIZE_ARGS)
                        .argName(BATCH_POINT_SIZE_NAME)
                        .hasArg()
                        .desc("100000 (optional)")
                        .build();
        options.addOption(opBatchPointSize);

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

        return options;
    }

    /**
     * parse special params
     *
     * @param commandLine
     */
    private static void parseSpecialParams(CommandLine commandLine) {
        timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
        targetPath = commandLine.getOptionValue(FILE_ARGS);
        if (commandLine.getOptionValue(BATCH_POINT_SIZE_ARGS) != null) {
            batchPointSize = Integer.parseInt(commandLine.getOptionValue(BATCH_POINT_SIZE_ARGS));
        }
        if (commandLine.getOptionValue(FAILED_FILE_ARGS) != null) {
            failedFileDirectory = commandLine.getOptionValue(FAILED_FILE_ARGS);
            File file = new File(failedFileDirectory);
            if (!file.isDirectory()) {
                file.mkdir();
                failedFileDirectory = file.getAbsolutePath() + File.separator;
            }
        }

        if (commandLine.getOptionValue(ALIGNED_ARGS) != null) {
            aligned = Boolean.valueOf(commandLine.getOptionValue(ALIGNED_ARGS));
        }

        String compressAlgorithm = commandLine.getOptionValue(COMPRESS_ARGS);
        if ("snappy".equals(compressAlgorithm)) {
            compressType = CompressType.SNAPPY;
        } else if( "gzip".equals(compressAlgorithm) ){
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
    }

    private static ImportModel generateImportCsvFile(){
        ImportModel importModel = new ImportModel();
        importModel.setCompressType(compressType);
        importModel.setAligned(aligned);
        importModel.setBatchPointSize(batchPointSize);
        importModel.setTargetPath(targetPath);
        importModel.setCharSet(charSet);
        return importModel;
    }
}
