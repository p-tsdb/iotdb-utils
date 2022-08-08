package org.apache.iotdb.utils.tools.integration;

import org.apache.iotdb.utils.tools.ExportCsvNew;

public class tt {
    public static void main(String[] args) {
        String[] ab = new String[20];
        ab[0] = "-h";
        ab[1] = "127.0.0.1";
        ab[2] = "-p";
        ab[3] = "6667";
        ab[4] = "-u";
        ab[5] = "root";
        ab[6] = "-pw";
        ab[7] = "root";
        ab[8] = "-tf";
        ab[9] = "timestamp";
        ab[10] = "-td";
        ab[11] = "./";
        ab[12] = "-q";
        ab[13] = "select * from root.test.yonyou.cli.monitor";
        ab[14] = "-cs";
        ab[15] = "utf8";
        ab[16] = "-ap";
        ab[17] = "true";
        ab[18] = "-f";
        ab[19] = "xx";


        ExportCsvNew.main(ab);
    }
}
