package de.cms;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;

public class Fetcher implements Serializable{
    private static final Logger LOG = LogManager.getLogger(PartsMapApp.class);
    private static String AGENT = "Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; de-de) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405";

    public String fetch(String url){
        URL u;
        StringWriter writer = new StringWriter();
        try {
            u = new URL(url);
            URLConnection con = u.openConnection();
            con.setConnectTimeout(20000);
            con.setReadTimeout(20000);
            IOUtils.copy(con.getInputStream(), writer);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
        return writer.toString();
    }
}
