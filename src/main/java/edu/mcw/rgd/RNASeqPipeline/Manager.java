package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by cdursun on 5/19/2017.
 */
public class Manager {
    private String version;
    private final static Logger loggerSummary = LogManager.getLogger("summary");

    private RnaSeqToRgdMapper rnaSeqToRgdMapper;
    private byte numberOfMapperThreads;
    private byte numberOfDownloaderThreads;
    private int indexOfStopFolderForDownload;
    private byte downloaderMaxRetryCount;
    private byte downloaderDownloadRetryIntervalInSeconds;
    private int indexOfStartFolderForDownload;
    private boolean performDownload;
    private boolean performMapping;
    private String ncbiSoftFilesFtpLink;


    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        Manager manager= (Manager) bf.getBean("main");
        manager.init(bf);

        Date time0 = new Date();
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
            // set cutoff date for ontology analysis to Apr 1, 2023
            Calendar cal = Calendar.getInstance();
            cal.set(2023, 4-1, 1);
            Date analysisCutoffDate = cal.getTime();

            manager.run(analysisCutoffDate);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
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
        ExecutorService executor = Executors.newFixedThreadPool(numberOfDownloaderThreads);

        
        Object[] threadData = new Object[numberOfDownloaderThreads];
        for (int i = 0; i < numberOfDownloaderThreads ; i++) {
            threadData[i] = new ArrayList<Integer>();
        }
        int threadSlot = -1;
        for( int folderIndex = indexOfStartFolderForDownload; folderIndex <= indexOfStopFolderForDownload; folderIndex++ ) {
            threadSlot = (threadSlot + 1) % threadData.length;

            List<Integer> list = (List<Integer>) threadData[threadSlot];
            list.add( folderIndex );
        }

        for (int i = 0; i < numberOfDownloaderThreads ; i++) {
            System.out.println("Starting thread "+ i);
            List<Integer> folderIndexList = (List<Integer>) threadData[i];

            DownloaderThread thread = new DownloaderThread(i,
                    new SoftFileDownloader(downloaderMaxRetryCount, downloaderDownloadRetryIntervalInSeconds, counters),
                    new SoftFileParser(), new RnaSeqDAO(), folderIndexList);

            executor.execute(thread);
        }

        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

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

    public void setNumberOfDownloaderThreads(byte numberOfDownloaderThreads) {
        this.numberOfDownloaderThreads = numberOfDownloaderThreads;
    }

    public byte getNumberOfDownloaderThreads() {
        return numberOfDownloaderThreads;
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
