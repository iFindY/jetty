
package de.arkadi.service;

import de.arkadi.resources.sites.model.Site;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

public class SitesFileExtractor implements Runnable{
    private final String DATA_FILE_PATH;
    private final BlockingQueue<Site> sites;
    private volatile boolean isComplete = false;
    private volatile boolean stopProcessing = false;

    /**
     * 
     * @param DATA_FILE_PATH the file should be a csv.zip with [unsigned int, string], eg 1, google.com
     * @param sites 
     */
    public SitesFileExtractor(String DATA_FILE_PATH, BlockingQueue<Site> sites) {
        this.DATA_FILE_PATH = DATA_FILE_PATH;
        this.sites = sites;
    }
    
    /**
     * Parse top sites csv.zip and push elements to queue
     */
    protected void parseFile() {
        try (ZipFile zip = new ZipFile(DATA_FILE_PATH);
                InputStreamReader isr = new InputStreamReader(zip.getInputStream(zip.getEntry("top-1m.csv")));
                BufferedReader br = new BufferedReader(isr)) {

            String line = null;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");
                try {
                    sites.add(new Site(Integer.parseInt(columns[0]), columns[1]));
                } catch (NumberFormatException ex) {
                    Logger.getLogger(SitesRedisExtractor.class.getName()).log(Level.SEVERE, null, ex);
                }

                //Wait until queue is processing
                int timeout = 2;
                while (sites.size() > 10000 && !stopProcessing) {
                    Thread.sleep(timeout);
                    timeout *= timeout;
                }

                if (stopProcessing) {
                    sites.add(new SitePoison());
                    return;
                }
            }
            
            isComplete = true;
            
            //This is our posisen object, to stop queue listening
            sites.add(new SitePoison());
            return;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(SitesFileExtractor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(SitesFileExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        sites.add(new SitePoison());
    }

    public boolean isIsComplete() {
        return isComplete;
    }

    public void setIsComplete(boolean isComplete) {
        this.isComplete = isComplete;
    }

    public boolean isStopProcessing() {
        return stopProcessing;
    }

    public void setStopProcessing(boolean stopProcessing) {
        this.stopProcessing = stopProcessing;
    }

    @Override
    public void run() {
        Logger.getLogger(SitesFileExtractor.class.getName()).log(Level.INFO, "======= File extractor started");
        
        parseFile();
        
        Logger.getLogger(SitesFileExtractor.class.getName()).log(Level.INFO, "======= File extractor completed");
    }
}
