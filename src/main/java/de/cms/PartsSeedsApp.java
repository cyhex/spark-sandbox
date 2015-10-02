package de.cms;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class PartsSeedsApp {

    private static final Logger log = LogManager.getLogger(PartsMapApp.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PartsSeedsApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Document mainSiteMap = null;
        try {
            mainSiteMap = Jsoup.connect("http://www.partsrunner.de/sitemaps/sitemap.xml").get();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        List<String> loc = mainSiteMap.select("loc").stream().filter(Element::hasText)
                .map(Element::text).collect(Collectors.toList());

        JavaRDD<String> seedsRdd = sc.parallelize(loc)
                .flatMap(url -> Jsoup.connect(url).get().select("loc").stream()
                                .map(Element::text).collect(Collectors.toList())
                );

        log.warn("#####################################");
        log.warn("found seeds: " + seedsRdd.count());
        seedsRdd.saveAsTextFile("/home/gx/Desktop/seeds.rdd");

    }

}