package edu.mcw.rgd.RNASeqPipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.Calendar;

/**
 * Created by cdursun on 7/24/2017.
 */
public class DownloaderThread implements Runnable {
    private final static Log loggerSummary;
    private int threadNum;
    private int startIndexForFolder;
    private int stopIndexForFolder;
    private  int maxNumOfFilesPerFolderOnNcbi;
    private RnaSeqDAO rnaSeqDao;
    private SoftFileDownloader softFileDownloader;
    private SoftFileParser softFileParser;
    static {
        loggerSummary = LogFactory.getLog("summary");
    }
    public DownloaderThread(int threadNum, SoftFileDownloader softFileDownloader, SoftFileParser softFileParser, RnaSeqDAO rnaSeqDao, int maxNumOfFilesPerFolderOnNcbi, int startIndexForFolder, int stopIndexForFolder){
        this.threadNum = threadNum;
        this.rnaSeqDao = rnaSeqDao;
        this.softFileDownloader = softFileDownloader;
        this.maxNumOfFilesPerFolderOnNcbi = maxNumOfFilesPerFolderOnNcbi;
        this.startIndexForFolder = startIndexForFolder;
        this.stopIndexForFolder = stopIndexForFolder;
        this.softFileParser = softFileParser;

    }

   /* public void run() {
        loggerSummary.info("DownloaderThread-" + threadNum + " => started interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", start stop files: " + startIndexForFile + "-" + stopIndexForFile +
                ", time: " + Calendar.getInstance().getTime());

        int j = startIndexForFile;

        /*if (threadNum == 0) // for the first thread the starting file index can be set to different value other than 0
            j = startIndexForFile;*/

      /*  String softFileName;
        for (int i = startIndexForFolder; i < stopIndexForFolder; i++) {
            for (; j < maxNumOfFilesPerFolderOnNcbi; j++) {

                //stopIndexForFile can only be different only for last Thread
                //for the last Thread if it is the last file to be processed then break the loop
                if (stopIndexForFile != maxNumOfFilesPerFolderOnNcbi && i == (stopIndexForFolder-1) && j == (stopIndexForFile + 1)) {
                    break;
                }

                softFileName = softFileDownloader.downloadAndExtractSoftFile(i, j);

                if (softFileName == null ) continue;

                File file = new File(softFileName);

                Series series = softFileParser.parse(file);
                if (series != null)
                    rnaSeqDao.insertRnaSeq(series);
                else
                    loggerSummary.error("Parse error : " + softFileName );

                file.delete();

            }
            // set the j index(for file) to 0 after processing first folder
            j = 0;
        }
        loggerSummary.info("DownloaderThread-" + threadNum + " => finished interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", start stop files: " + startIndexForFile + "-" + stopIndexForFile +
                ", time: " + Calendar.getInstance().getTime());
    }*/

    public void run() {
        loggerSummary.info("DownloaderThread-" + threadNum + " => started interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", time: " + Calendar.getInstance().getTime());


        String softFileName;
        for (int i = startIndexForFolder; i < stopIndexForFolder; i++) {

            String directoryName = SoftFileDownloader.getNcbiDirectoryName(i);

            softFileDownloader.setExternalFile( SoftFileDownloader.getGeoSoftFilesFtpLink()+ directoryName);
            String[] fileAccIds = null;
            try {
                fileAccIds = softFileDownloader.listFiles();
            } catch (Exception e) {
                loggerSummary.error("Directory list error : Skipping directory " + softFileDownloader.getExternalFile() );
                continue;
            }

            for (int j=0; j < fileAccIds.length; j++) {

                softFileName = softFileDownloader.downloadAndExtractSoftFile(directoryName, fileAccIds[j]);

                if (softFileName == null ) continue;

                File file = new File(softFileName);

                Series series = softFileParser.parse(file);
                if (series != null)
                    rnaSeqDao.insertRnaSeq(series);
                else
                    loggerSummary.error("Parse error : " + softFileName );

                file.delete();

            }
        }
        loggerSummary.info("DownloaderThread-" + threadNum + " => finished interval folders: " + startIndexForFolder +
                "-" + stopIndexForFolder + ", time: " + Calendar.getInstance().getTime());
    }

}
