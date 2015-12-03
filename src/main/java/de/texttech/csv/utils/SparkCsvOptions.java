package de.texttech.csv.utils;

import java.util.HashMap;

/**
 * https://github.com/databricks/spark-csv
 */
public class SparkCsvOptions extends HashMap<String, String> {

    public static String MODE_PERMISSIVE = "PERMISSIVE";
    public static String MODE_DROPMALFORMED = "DROPMALFORMED";
    public static String MODE_FAILFAST = "FAILFAST";

    public SparkCsvOptions() {
        setHeader(true);
        setDelimiter(",");
        setCharset("UTF-8");
        setInferSchema(true);
        setModePermissive();
    }

    public void setHeader(boolean header) {
        put("header", header ? "true" : "false");
    }

    public void setDelimiter(String delimiter) {
        put("delimiter", delimiter);
    }

    public void setQuote(String quote) {
        put("quote", quote);
    }

    public void setCharset(String charset) {
        put("charset", charset);
    }

    public void setModePermissive() {
        put("mode", MODE_PERMISSIVE);
    }

    public void setModeDropmalformed() {
        put("mode", MODE_DROPMALFORMED);
    }

    public void setModeFailfast() {
        put("mode", MODE_FAILFAST);
    }

    public void setInferSchema(boolean inferSchema) {
        put("inferSchema", inferSchema ? "true" : "false");
    }

}
