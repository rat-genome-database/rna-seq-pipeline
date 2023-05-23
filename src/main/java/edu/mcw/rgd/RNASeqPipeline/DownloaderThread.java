package edu.mcw.rgd.RNASeqPipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by cdursun on 7/24/2017.
 */
public class DownloaderThread implements Runnable {
    private final static Logger loggerSummary = LogManager.getLogger("summary");
    private int threadNum;
    private int startIndexForFolder;
    private int stopIndexForFolder;
    private int maxNumOfFilesPerFolderOnNcbi;
    private RnaSeqDAO rnaSeqDao;
    private SoftFileDownloader softFileDownloader;
    private SoftFileParser softFileParser;

    public DownloaderThread(int threadNum, SoftFileDownloader softFileDownloader, SoftFileParser softFileParser, RnaSeqDAO rnaSeqDao, int maxNumOfFilesPerFolderOnNcbi, int startIndexForFolder, int stopIndexForFolder){
        this.threadNum = threadNum;
        this.rnaSeqDao = rnaSeqDao;
        this.softFileDownloader = softFileDownloader;
        this.maxNumOfFilesPerFolderOnNcbi = maxNumOfFilesPerFolderOnNcbi;
        this.startIndexForFolder = startIndexForFolder;
        this.stopIndexForFolder = stopIndexForFolder;
        this.softFileParser = softFileParser;

    }

    public void run() {
        loggerSummary.info("DownloaderThread-" + threadNum + " => started interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", time: " + Calendar.getInstance().getTime());

        System.out.println("DownloaderThread-" + threadNum + " => started interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", time: " + Calendar.getInstance().getTime());
        String softFileName;
        for (int i = startIndexForFolder; i < stopIndexForFolder; i++) {

            String directoryName = SoftFileDownloader.getNcbiDirectoryName(i);

            softFileDownloader.setExternalFile( SoftFileDownloader.getGeoSoftFilesFtpLink()+ directoryName);
            String[] fileAccIds = null;
            List<String> existingIds = new ArrayList<>();
            List<String> loaded = new ArrayList<>();
            try {
                fileAccIds = softFileDownloader.listFiles();
                existingIds = rnaSeqDao.getGeoIds("GSE"+i+"%");
            } catch (Exception e) {
                loggerSummary.error("Directory list error : Skipping directory " + softFileDownloader.getExternalFile() );
                continue;
            }

            for (String fileAccId : fileAccIds) {
                if(!existingIds.contains(fileAccId)) {
                    loaded.add(fileAccId);
                    softFileName = softFileDownloader.downloadAndExtractSoftFile(directoryName, fileAccId);
                    System.out.println(softFileName);
                    if (softFileName == null) continue;
                    File file = new File(softFileName);
                    Series series = softFileParser.parse(file);
                    if (series != null)
                        rnaSeqDao.insertRnaSeq(series);
                    else
                        loggerSummary.error("Parse error : " + softFileName);
                    file.delete();

                    loggerSummary.info("Updated: "+series.getGeoAccessionID());
                }
            }
            loggerSummary.info("Loaded for folder " + directoryName+ " : "+ loaded.size());
        }
        loggerSummary.info("DownloaderThread-" + threadNum + " => finished interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", time: " + Calendar.getInstance().getTime());
    }

}
