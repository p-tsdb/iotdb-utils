package org.apache.iotdb.utils.core.unit;

import org.apache.iotdb.utils.core.CsvFileTransformation;
import org.apache.iotdb.utils.core.parse.CsvFileTransParser;
import org.apache.iotdb.utils.core.script.CommonScript;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CsvTransformTest extends CommonScript {

    @Test
    public void testAliCsv() throws Exception {
        String resourceFilePath = getResourceFilePath();
        String csvFileNameIm = "ali.csv";
        CsvFileTransParser parser =
                CsvFileTransformation.getCsvFileTransParser(resourceFilePath + "\\" + csvFileNameIm,"utf8");
        try {
            List<String> l1 = parser.loadInsertStrList(2, false);
            List<String> l2 = parser.loadInsertStrList(2, true);
            List<String> l3 = parser.loadInsertStrList(2, true);
            List<String> l4 = parser.loadInsertStrList(5, true);
            long num = l1.size() + l2.size() + l3.size() + l4.size();
            //l1.stream().forEach(s -> System.out.print("-----------" + s));
            //l2.stream().forEach(s -> System.out.print("-----------" + s));
            //l3.stream().forEach(s -> System.out.print("-----------" + s));
            //l4.stream().forEach(s -> System.out.print("-----------" + s));
            //System.out.println("--------------nums-----" + num);
            Assert.assertEquals(10, num);
        } catch (Exception e) {
            System.out.println("test failed, because:" + e.getMessage());
            fail();
        } finally {
            parser.close();
        }
    }

    @Test
    public void testImportCsv() throws Exception {
        String resourceFilePath = getResourceFilePath();
        String csvFileNameIm = "import.csv";
        CsvFileTransParser parser =
                CsvFileTransformation.getCsvFileTransParser(resourceFilePath + "\\" + csvFileNameIm,"utf8");
        try {
            List<String> l1 = parser.loadInsertStrList(1000, false);
            List<String> l2 = parser.loadInsertStrList(2000, true);
            List<String> l3 = parser.loadInsertStrList(30000, true);
            List<String> l4 = parser.loadInsertStrList(40000, true);
            long num = l1.size() + l2.size() + l3.size() + l4.size();
            //l1.stream().forEach(s -> System.out.print("-----------" + s));
            //l2.stream().forEach(s -> System.out.print("-----------" + s));
            //l3.stream().forEach(s -> System.out.print("-----------" + s));
            //l4.stream().forEach(s -> System.out.print("-----------" + s));
            //System.out.println("--------------nums-----" + num);
            Assert.assertEquals(45000, num);
        } catch (Exception e) {
            System.out.println("test failed, because:" + e.getMessage());
            fail();
        } finally {
            parser.close();
        }
    }

    @Test
    public void testImportAlignByDevice() throws Exception {
        String resourceFilePath = getResourceFilePath();
        String csvFileNameIm = "importAlignByDevice.csv";
        CsvFileTransParser parser =
                CsvFileTransformation.getCsvFileTransParser(resourceFilePath + "\\" + csvFileNameIm,"utf8");
        try {
            List<String> l1 = parser.loadInsertStrList(1000, false);
            List<String> l2 = parser.loadInsertStrList(2000, true);
            List<String> l3 = parser.loadInsertStrList(30000, true);
            List<String> l4 = parser.loadInsertStrList(40000, true);
            long num = l1.size() + l2.size() + l3.size() + l4.size();
            //l1.stream().forEach(s -> System.out.print("-----------" + s));
            //l2.stream().forEach(s -> System.out.print("-----------" + s));
            //l3.stream().forEach(s -> System.out.print("-----------" + s));
            //l4.stream().forEach(s -> System.out.print("-----------" + s));
            //System.out.println("--------------nums-----" + num);
            Assert.assertEquals(45000, num);
        } catch (Exception e) {
            System.out.println("test failed, because:" + e.getMessage());
            fail();
        } finally {
            parser.close();
        }
    }

    @Test
    public void testStreamAliCsv() throws Exception {
        String resourceFilePath = getResourceFilePath();
        String csvFileNameIm = "ali.csv";
        InputStream inputStream =
                CsvFileTransformation.transCsvToInsertStream(
                        resourceFilePath + "\\" + csvFileNameIm, true,"utf8");
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            int nums = 0;
            int b;
            while ((b = inputStreamReader.read()) != -1) {
                char cb = (char) b;
                if (cb == '\n') {
                    nums++;
                }
            }
            assertEquals(10, nums);
        } finally {
            inputStream.close();
        }
    }

    @Test
    public void testStreamImportCsv() throws Exception {
        String resourceFilePath = getResourceFilePath();
        String csvFileNameIm = "import.csv";
        InputStream inputStream =
                CsvFileTransformation.transCsvToInsertStream(
                        resourceFilePath + "\\" + csvFileNameIm, true,"utf8");
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            int nums = 0;
            int b;
            while ((b = inputStreamReader.read()) != -1) {
                char cb = (char) b;
                if (cb == '\n') {
                    nums++;
                }
            }
            assertEquals(45000, nums);
        } finally {
            inputStream.close();
        }
    }

    @Test
    public void testStreamImportAlignByDevice() throws Exception {
        String resourceFilePath = getResourceFilePath();
        String csvFileNameIm = "importAlignByDevice.csv";
        InputStream inputStream =
                CsvFileTransformation.transCsvToInsertStream(
                        resourceFilePath + "\\" + csvFileNameIm, true,"utf8");
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            int nums = 0;
            int b;
            while ((b = inputStreamReader.read()) != -1) {
                char cb = (char) b;
                if (cb == '\n') {
                    nums++;
                }
            }
            assertEquals(45000, nums);
        } finally {
            inputStream.close();
        }
    }
}
