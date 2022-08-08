package org.apache.iotdb.utils.tools.integration;

import org.apache.iotdb.utils.tools.ImportCsvNew;

public class ttttt {


    public static void main(String[] args) {
        String aa = "-h 127.0.0.1 -p 6667 -u root -pw root -f D:\\workspace\\iotdb-utils\\tools\\target\\utils-tools-0.13.0-SNAPSHOT\\tools\\dump0.csv.gzip -fd .\\failed -c gzip";
        String[] aarray = aa.split(" ");
        ImportCsvNew.main(aarray);
    }
}
