package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.process.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by cdursun on 5/19/2017.
 */
public class Manager {
    private String version;
    private final static Log loggerSummary;
    private SoftFileDownloader softFileDownloader;
    static {
        loggerSummary = LogFactory.getLog("summary");
    }

    private RnaSeqToRgdMapper rnaSeqToRgdMapper;
    private byte numberOfMapperThreads;
    private byte numberOfDownloaderThreads;
    private int indexOfStopFolderForDownload;
    private int numberOfFilesPerFolderOnNcbi;
    private byte downloaderMaxRetryCount;
    private byte downloaderDownloadRetryIntervalInSeconds;
    private byte indexOfStartFolderForDownload;
    private boolean performDownload;
    private boolean performMapping;
    private String ncbiSoftFilesFtpLink;


    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        Manager manager= (Manager) bf.getBean("main");
        manager.init(bf);

        Date time0 = Calendar.getInstance().getTime();

        try {
            manager.run(time0);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        loggerSummary.info("========== Elapsed time " + Utils.formatElapsedTime(time0.getTime(), System.currentTimeMillis()) + ". ==========");
    }

    public void run(Date runDate) throws Exception {

        if (performDownload)
            downloadAndInsertRNASeqData();

        if (performMapping)
            mapRnaSeqToRgd();

       /* String input = "from, HeLa cell cytoplasmic extracts atria doing surgeries multi-unit " +
                "eyes exocrine pancreas subdivision of organism along the main body axis Leydig's organ " +
                "mixed ectoderm/mesoderm/endoderm-derived structure amenities conspirator are playing";
        input = "Drosophila wandering larvae leg imaginal discs";
        //System.out.println(rnaSeqToRgdMapper.lemmatize(input));
       System.out.println(rnaSeqToRgdMapper.lemmatize(input));*/
    }

    public void mapRnaSeqToRgd() throws Exception{
        rnaSeqToRgdMapper.init();
        ExecutorService executor = Executors.newFixedThreadPool(numberOfMapperThreads);

        final int offset = rnaSeqToRgdMapper.getRnaSeqList().size() / numberOfMapperThreads;

        for (int i = 0; i < numberOfMapperThreads ; i++) {
            final int startIndex = i * offset;
            int stopIndex = startIndex + offset;
            if (i == (numberOfMapperThreads - 1))
                stopIndex = rnaSeqToRgdMapper.getRnaSeqList().size();
            executor.execute(new MapperThread(i, rnaSeqToRgdMapper, startIndex, stopIndex));
        }

        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

        rnaSeqToRgdMapper.applyMappingToDb();
    }

    private void downloadAndInsertRNASeqData() throws Exception{

        SoftFileDownloader.setGeoSoftFilesFtpLink(ncbiSoftFilesFtpLink);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfDownloaderThreads);

        final int offset = (indexOfStopFolderForDownload - indexOfStartFolderForDownload) / numberOfDownloaderThreads;

        for (int i = 0; i < numberOfDownloaderThreads ; i++) {
            final int folderStartIndex = i * offset + indexOfStartFolderForDownload;
            int folderStopIndex = folderStartIndex + offset;

            if (i == (numberOfDownloaderThreads - 1)) //set stop folder index for last thread
                folderStopIndex = indexOfStopFolderForDownload;


            DownloaderThread thread = new DownloaderThread(i, new SoftFileDownloader(downloaderMaxRetryCount,
                    downloaderDownloadRetryIntervalInSeconds), new SoftFileParser(), new RnaSeqDAO(),
                    numberOfFilesPerFolderOnNcbi, folderStartIndex, folderStopIndex);

            executor.execute(thread);
        }

        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

//        loggerSummary.info("Total number of files downloaded: " + softFileDownloader.getNumberOfDownloadedFiles());
//        loggerSummary.info("Total number of empty files : " + softFileDownloader.getNumberOfEmptyFiles());
    }


    /*private void downloadAndInsertRNASeqData() throws Exception{
        rnaSeqDao = new RnaSeqDAO();
        int i = iIndex;
        int j = jIndex;
        String softFileName;
        for (; i <= MAX_NUM_OF_FOLDERS_ON_NCBI; i++) {
            for (; j <= MAX_NUM_OF_FILES_PER_FOLDER_ON_NCBI; j++) {
                softFileName = softFileDownloader.downloadSoftFile(i, j);

                if (softFileName == null ) continue;

                File file = new File(softFileName);

                SoftFileParser.parse(file);
                rnaSeqDao.insertRnaSeq(SoftFileParser.allSOFTs.get(0));
                SoftFileParser.reset();
                file.delete();
            }
            j = 0;
        }
        loggerSummary.info("Total number of files downloaded: " + softFileDownloader.getNumberOfDownloadedFiles());
        loggerSummary.info("Total number of empty files : " + softFileDownloader.getNumberOfEmptyFiles());
    } */

    void init(DefaultListableBeanFactory bf) {
        loggerSummary.info(getVersion());
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }


    public void setSoftFileDownloader(SoftFileDownloader softFileDownloader) {
        this.softFileDownloader = softFileDownloader;
    }

    public SoftFileDownloader getSoftFileDownloader() {
        return softFileDownloader;
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

    public void setIndexOfStopFolderForDownload(int indexOfStopFolderForDownload) {
        this.indexOfStopFolderForDownload = indexOfStopFolderForDownload;
    }

    public int getIndexOfStopFolderForDownload() {
        return indexOfStopFolderForDownload;
    }

    public void setNumberOfFilesPerFolderOnNcbi(int numberOfFilesPerFolderOnNcbi) {
        this.numberOfFilesPerFolderOnNcbi = numberOfFilesPerFolderOnNcbi;
    }

    public int getNumberOfFilesPerFolderOnNcbi() {
        return numberOfFilesPerFolderOnNcbi;
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

    public void setIndexOfStartFolderForDownload(byte indexOfStartFolderForDownload) {
        this.indexOfStartFolderForDownload = indexOfStartFolderForDownload;
    }

    public byte getIndexOfStartFolderForDownload() {
        return indexOfStartFolderForDownload;
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
}
