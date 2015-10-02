package de.cms;

import org.apache.http.client.fluent.Request;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

public class Fetcher implements Serializable{
    private static final Logger log = LogManager.getLogger(PartsMapApp.class);
    private static String AGENT = "Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; de-de) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405";

    public String fetch(String url){
        try {
            return Request.Get(url)
                    .connectTimeout(10000)
                    .socketTimeout(10000)
                    .userAgent(AGENT)
                    .execute().returnContent().asString();
        } catch (IOException e) {
            log.warn(e.getMessage());
            return null;
        }
    }
}
