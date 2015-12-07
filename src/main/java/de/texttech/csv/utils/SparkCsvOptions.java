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
        setInferSchema(false);
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

    /**
     * tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
     */
    public void setModePermissive() {
        put("mode", MODE_PERMISSIVE);
    }

    /**
     * drops lines which have fewer or more tokens than expected or tokens which do not match the schema
     */
    public void setModeDropmalformed() {
        put("mode", MODE_DROPMALFORMED);
    }

    /**
     * drops lines which have fewer or more tokens than expected or tokens which do not match the schema
     */
    public void setModeFailfast() {
        put("mode", MODE_FAILFAST);
    }

    /**
     * automatically infers column types. It requires one extra pass over the data and is false by default
     * @param inferSchema default false
     */
    public void setInferSchema(boolean inferSchema) {
        put("inferSchema", inferSchema ? "true" : "false");
    }

}
