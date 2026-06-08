package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by cdursun on 5/19/2017.
 */
public class Manager {
    private String version;
    private final static Logger loggerSummary = LogManager.getLogger("summary");

    private RnaSeqToRgdMapper rnaSeqToRgdMapper;
    private byte numberOfMapperThreads;
    private int indexOfStopFolderForDownload;
    private byte downloaderMaxRetryCount;
    private byte downloaderDownloadRetryIntervalInSeconds;
    private int indexOfStartFolderForDownload;
    private boolean performDownload;
    private boolean performMapping;
    private String ncbiSoftFilesFtpLink;
    private String analysisCutoffDate;


    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        Manager manager= (Manager) bf.getBean("main");
        manager.init(bf);

        Date time0 = new Date();

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

        for( int i=0; i<args.length; i++ ) {
            String arg = args[i];
            switch (arg) {
                case "--start":
                    manager.indexOfStartFolderForDownload = Integer.parseInt(args[++i]);
                    break;
                case "--stop":
                    manager.indexOfStopFolderForDownload = Integer.parseInt(args[++i]);
                    break;
            }
        }
        try {
            // parse cutoff date from AppConfigure.xml, e.g. "Apr 1, 2023"
            SimpleDateFormat sdf = new SimpleDateFormat("MMM d, yyyy", Locale.ENGLISH);
            Date analysisCutoffDate = sdf.parse(manager.getAnalysisCutoffDate());
            loggerSummary.info("Analysis cutoff date: " + sdf.format(analysisCutoffDate));

            manager.run(analysisCutoffDate);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            memoryMonitor.stop();
            loggerSummary.info(memoryMonitor.getSummary());
        }

        loggerSummary.info("========== Elapsed time " + Utils.formatElapsedTime(time0.getTime(), System.currentTimeMillis()) + ". ==========");
    }

    public void run(Date analysisCutoffDate) throws Exception {

        if (performDownload) {
            try {
                downloadAndInsertRNASeqData();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        if (performMapping) {
            // (re)map all rows created after the cutoff date
            mapRnaSeqToRgd( analysisCutoffDate );
        }

       /* String input = "from, HeLa cell cytoplasmic extracts atria doing surgeries multi-unit " +
                "eyes exocrine pancreas subdivision of organism along the main body axis Leydig's organ " +
                "mixed ectoderm/mesoderm/endoderm-derived structure amenities conspirator are playing";
        input = "Drosophila wandering larvae leg imaginal discs";
        //System.out.println(rnaSeqToRgdMapper.lemmatize(input));
       System.out.println(rnaSeqToRgdMapper.lemmatize(input));*/
    }

    public void mapRnaSeqToRgd(Date dateCutoff) throws Exception {

        rnaSeqToRgdMapper.init(dateCutoff);

        rnaSeqToRgdMapper.getRnaSeqList().parallelStream().forEach( r -> {
            rnaSeqToRgdMapper.mapRnaSeqToRgd(r);
        });
        loggerSummary.info("Total number of records after Lemmatization : " + rnaSeqToRgdMapper.getNumberOfMappingsAfterLemmatization());
    }

    private void downloadAndInsertRNASeqData() throws Exception{

        CounterPool counters = new CounterPool();

        SoftFileDownloader.setGeoSoftFilesFtpLink(ncbiSoftFilesFtpLink);

        // downloads run one folder at a time (single-threaded by design, to be gentle on NCBI)
        SoftFileLoader loader = new SoftFileLoader(
                new SoftFileDownloader(downloaderMaxRetryCount, downloaderDownloadRetryIntervalInSeconds, counters),
                new SoftFileParser(), new RnaSeqDAO());

        for( int folderIndex = indexOfStartFolderForDownload; folderIndex <= indexOfStopFolderForDownload; folderIndex++ ) {
            try {
                loader.processFolder(folderIndex);
            } catch( Exception e ) {
                // one folder's unexpected failure should not abort the rest of the run
                loggerSummary.error("Folder processing error : skipping folder index " + folderIndex, e);
            }
        }

        loggerSummary.info("Total number of files downloaded: " + counters.get("numberOfDownloadedFiles"));
        loggerSummary.info("Total number of empty files : " + counters.get("numberOfEmptyFiles"));
    }



    void init(DefaultListableBeanFactory bf) {
        loggerSummary.info(getVersion());
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setRnaSeqToRgdMapper(RnaSeqToRgdMapper rnaSeqToRgdMapper) {
        this.rnaSeqToRgdMapper = rnaSeqToRgdMapper;
    }

    public RnaSeqToRgdMapper getRnaSeqToRgdMapper() {
        return rnaSeqToRgdMapper;
    }

    public void setNumberOfMapperThreads(byte numberOfMapperThreads) {
        this.numberOfMapperThreads = numberOfMapperThreads;
    }

    public byte getNumberOfMapperThreads() {
        return numberOfMapperThreads;
    }

    public void setDownloaderMaxRetryCount(byte downloaderMaxRetryCount) {
        this.downloaderMaxRetryCount = downloaderMaxRetryCount;
    }

    public byte getDownloaderMaxRetryCount() {
        return downloaderMaxRetryCount;
    }

    public void setDownloaderDownloadRetryIntervalInSeconds(byte downloaderDownloadRetryIntervalInSeconds) {
        this.downloaderDownloadRetryIntervalInSeconds = downloaderDownloadRetryIntervalInSeconds;
    }

    public byte getDownloaderDownloadRetryIntervalInSeconds() {
        return downloaderDownloadRetryIntervalInSeconds;
    }

    public void setPerformDownload(boolean performDownload) {
        this.performDownload = performDownload;
    }

    public boolean getPerformDownload() {
        return performDownload;
    }

    public void setPerformMapping(boolean performMapping) {
        this.performMapping = performMapping;
    }

    public boolean getPerformMapping() {
        return performMapping;
    }

    public void setNcbiSoftFilesFtpLink(String ncbiSoftFilesFtpLink) {
        this.ncbiSoftFilesFtpLink = ncbiSoftFilesFtpLink;
    }

    public String getNcbiSoftFilesFtpLink() {
        return ncbiSoftFilesFtpLink;
    }

    public void setAnalysisCutoffDate(String analysisCutoffDate) {
        this.analysisCutoffDate = analysisCutoffDate;
    }

    public String getAnalysisCutoffDate() {
        return analysisCutoffDate;
    }

    public int getIndexOfStopFolderForDownload() {
        return indexOfStopFolderForDownload;
    }

    public void setIndexOfStopFolderForDownload(int indexOfStopFolderForDownload) {
        this.indexOfStopFolderForDownload = indexOfStopFolderForDownload;
    }

    public int getIndexOfStartFolderForDownload() {
        return indexOfStartFolderForDownload;
    }

    public void setIndexOfStartFolderForDownload(int indexOfStartFolderForDownload) {
        this.indexOfStartFolderForDownload = indexOfStartFolderForDownload;
    }

    public boolean isPerformDownload() {
        return performDownload;
    }

    public boolean isPerformMapping() {
        return performMapping;
    }
}
