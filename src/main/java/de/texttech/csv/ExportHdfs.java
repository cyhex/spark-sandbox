package de.texttech.csv;

import java.io.IOException;
import java.net.URISyntaxException;

public class ExportHdfs {

    public static void main(String[] args) throws URISyntaxException, IOException {
        HdfsHelpers.copyMerge("data/d2.csv", "data/d2.merged.csv");
    }

}
