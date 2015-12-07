package de.texttech.csv;

import de.texttech.csv.utils.SparkCsvOptions;
import de.texttech.csv.utils.SparkTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvStats extends SparkTask {
    private static final Logger log = LogManager.getLogger(CsvStats.class);
    private final SQLContext sqlContext;

    public CsvStats(JavaSparkContext sc) {
        super(sc);
        sqlContext = new SQLContext(sc);
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(CsvStats.class.getSimpleName())
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        CsvStats app = new CsvStats(sc);
        app.run();

    }

    @Override
    public void run() {

        List<StatsPayload> statsData = new ArrayList<>();

        SparkCsvOptions options = new SparkCsvOptions();
        options.setHeader(false);
        options.setInferSchema(true);
        DataFrame csv = sqlContext.read().format("com.databricks.spark.csv")
                .options(options)
                .load("data/d3.csv");

        // count unique ids
        statsData.add(new StatsPayload("unique_id", String.valueOf(csv.select("C0").distinct().count())));

        // count values
        statsData.addAll(csv.groupBy("C1").count().javaRDD()
                .map(r -> new StatsPayload(r.getString(0) + "_count", String.valueOf(r.getLong(1))))
                .collect());

        // avg values probs
        statsData.addAll(csv.groupBy("C1").avg("C3").javaRDD()
                .map(r -> new StatsPayload(r.getString(0) + "_avg", String.valueOf(r.getDouble(1))))
                .collect()
        );

        JavaRDD<StatsPayload> statsRDD = sc.parallelize(statsData, 1);
        DataFrame statsCsv = sqlContext.createDataFrame(statsRDD, StatsPayload.class);

        statsCsv.show();
        //statsCsv.save("data/xxx", SaveMode.Overwrite);

    }

    public static class StatsPayload implements Serializable {
        private String key;
        private String value;

        public StatsPayload(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
}
