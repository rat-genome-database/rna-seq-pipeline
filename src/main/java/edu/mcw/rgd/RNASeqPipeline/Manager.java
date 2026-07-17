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
    private byte downloaderMaxRetryCount;
    private byte downloaderDownloadRetryIntervalInSeconds;
    private boolean performDownload;
    private boolean performMapping;
    private String ncbiSoftFilesFtpLink;
    private String analysisCutoffDate;


    public static void main(String[] args) throws Exception {

        // optional targeted run: '--gse GSE53960' loads and maps just that one series
        String gseAccId = parseGseAccId(args);

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        Manager manager= (Manager) bf.getBean("main");
        manager.init(bf);

        Date time0 = new Date();

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

        try {
            // parse cutoff date from AppConfigure.xml, e.g. "Apr 1, 2023"
            SimpleDateFormat sdf = new SimpleDateFormat("MMM d, yyyy", Locale.ENGLISH);
            Date analysisCutoffDate = sdf.parse(manager.getAnalysisCutoffDate());
            loggerSummary.info("Analysis cutoff date: " + sdf.format(analysisCutoffDate));

            manager.run(analysisCutoffDate, gseAccId);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            memoryMonitor.stop();
            loggerSummary.info(memoryMonitor.getSummary());
        }

        loggerSummary.info("========== Elapsed time " + Utils.formatElapsedTime(time0.getTime(), System.currentTimeMillis()) + ". ==========");
    }

    public void run(Date analysisCutoffDate, String gseAccId) throws Exception {

        if( gseAccId != null ) {
            // targeted single-series run: load just this one series, then map just this one series
            // (no full GEO folder scan, no global remap of every pending row)
            loggerSummary.info("Single-series run for " + gseAccId);
            try {
                loadSingleSeries(gseAccId);
            } catch(Exception e) {
                e.printStackTrace();
            }
            if (performMapping) {
                mapRnaSeqToRgd( analysisCutoffDate, gseAccId );
            }
            return;
        }

        if (performDownload) {
            try {
                downloadAndInsertRNASeqData();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        if (performMapping) {
            // (re)map all rows created after the cutoff date
            mapRnaSeqToRgd( analysisCutoffDate, null );
        }
    }

    public void mapRnaSeqToRgd(Date dateCutoff, String gseAccId) throws Exception {

        if( gseAccId != null ) {
            rnaSeqToRgdMapper.initForGse(gseAccId);
        } else {
            rnaSeqToRgdMapper.init(dateCutoff);
        }

        rnaSeqToRgdMapper.getRnaSeqList().parallelStream().forEach( r -> {
            rnaSeqToRgdMapper.mapRnaSeqToRgd(r);
        });
        loggerSummary.info("Total number of records after Lemmatization : " + rnaSeqToRgdMapper.getNumberOfMappingsAfterLemmatization());
    }

    // '--gse GSE53960' -> "GSE53960"; '--gse=GSE53960' also accepted; null when the option is absent
    static String parseGseAccId(String[] args) {
        final String opt = "--gse";
        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals(opt) ) {
                if( i+1 >= args.length ) {
                    throw new IllegalArgumentException(opt+" requires a GSE accession, e.g. "+opt+" GSE53960");
                }
                return args[i+1].toUpperCase();
            }
            if( args[i].startsWith(opt+"=") ) {
                return args[i].substring(opt.length()+1).toUpperCase();
            }
        }
        return null;
    }

    private SoftFileDownloader newDownloader(CounterPool counters) {
        SoftFileDownloader.setGeoSoftFilesFtpLink(ncbiSoftFilesFtpLink);
        // downloads run one folder at a time (single-threaded by design, to be gentle on NCBI)
        return new SoftFileDownloader(downloaderMaxRetryCount, downloaderDownloadRetryIntervalInSeconds, counters);
    }

    /** load (and, if enabled, map) a single GEO series named on the command line */
    private void loadSingleSeries(String gseAccId) throws Exception {

        CounterPool counters = new CounterPool();
        SoftFileDownloader downloader = newDownloader(counters);
        SoftFileLoader loader = new SoftFileLoader(downloader, new SoftFileParser(), new RnaSeqDAO());

        loader.processSingleSeries(gseAccId);

        loggerSummary.info("Total number of files downloaded: " + counters.get("numberOfDownloadedFiles"));
    }

    private void downloadAndInsertRNASeqData() throws Exception{

        CounterPool counters = new CounterPool();

        SoftFileDownloader downloader = newDownloader(counters);
        SoftFileLoader loader = new SoftFileLoader(downloader, new SoftFileParser(), new RnaSeqDAO());

        // determine how many GEO grouping folders to download from the live root listing, so the run
        // always covers every series up to the newest one currently in GEO (no fixed start/stop props)
        int highestFolderIndex = downloader.getHighestFolderIndex();
        loggerSummary.info("Highest GEO series grouping folder: " + SoftFileDownloader.getNcbiDirectoryName(highestFolderIndex)
                + " (index " + highestFolderIndex + ")");

        for( int folderIndex = 0; folderIndex <= highestFolderIndex; folderIndex++ ) {
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

    public boolean isPerformDownload() {
        return performDownload;
    }

    public boolean isPerformMapping() {
        return performMapping;
    }
}
