package org.apache.iotdb.utils.core.script;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.thrift.annotation.Nullable;
import org.junit.Assert;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CommonScript {

  protected String getCliPath() {
    // This is usually always set by the JVM

    File userDir = new File(System.getProperty("user.dir"));
    if (!userDir.exists()) {
      throw new RuntimeException("user.dir " + userDir.getAbsolutePath() + " doesn't exist.");
    }
    File target = new File(userDir, "target/maven-archiver/pom.properties");
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(target));
    } catch (IOException e) {
      return "target/iotdb-cli-";
    }
    return new File(
            userDir,
            String.format(
                "target/%s-%s",
                properties.getProperty("artifactId"), properties.getProperty("version")))
        .getAbsolutePath();
  }

  protected String getResourceFilePath() {
    // This is usually always set by the JVM

    File userDir = new File(System.getProperty("user.dir"));
    if (!userDir.exists()) {
      throw new RuntimeException("user.dir " + userDir.getAbsolutePath() + " doesn't exist.");
    }
    File target = new File(userDir, "target/test-classes");
    return target.getAbsolutePath();
  }

  protected CSVParser csvParserStream(String filePath) throws IOException {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader()
        .setSkipHeaderRecord(true)
        .setQuote('`')
        .setEscape('\\')
        .setIgnoreEmptyLines(true)
        .build()
        .parse(new InputStreamReader(new FileInputStream(filePath)));
  }
}
