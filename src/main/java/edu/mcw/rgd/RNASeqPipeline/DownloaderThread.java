package edu.mcw.rgd.RNASeqPipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;

/**
 * Created by cdursun on 7/24/2017.
 */
public class DownloaderThread implements Runnable {
    private final static Logger loggerSummary = LogManager.getLogger("summary");
    private int threadNum;
    private List<Integer> indexForFolderList;
    private int maxNumOfFilesPerFolderOnNcbi;
    private RnaSeqDAO rnaSeqDao;
    private SoftFileDownloader softFileDownloader;
    private SoftFileParser softFileParser;

    public DownloaderThread(int threadNum, SoftFileDownloader softFileDownloader, SoftFileParser softFileParser, RnaSeqDAO rnaSeqDao,
                            int maxNumOfFilesPerFolderOnNcbi, List<Integer> indexForFolderList){
        this.threadNum = threadNum;
        this.rnaSeqDao = rnaSeqDao;
        this.softFileDownloader = softFileDownloader;
        this.maxNumOfFilesPerFolderOnNcbi = maxNumOfFilesPerFolderOnNcbi;
        this.indexForFolderList = indexForFolderList;
        this.softFileParser = softFileParser;

    }

    public void run() {
        String softFileName;
        for (int indexForFolder: indexForFolderList) {

            loggerSummary.info("DownloaderThread-" + threadNum + " => started interval folder: " + indexForFolder +
                    ", time: " + Calendar.getInstance().getTime());

            String directoryName = SoftFileDownloader.getNcbiDirectoryName(indexForFolder);

            softFileDownloader.setExternalFile( SoftFileDownloader.getGeoSoftFilesFtpLink()+ directoryName);
            String[] fileAccIds;
            Set<String> existingIds;
            List<String> loaded = new ArrayList<>();
            try {
                fileAccIds = softFileDownloader.listFiles();

                if( indexForFolder==0 ) {
                    existingIds = new HashSet<>();
                    existingIds.addAll( rnaSeqDao.getGeoIds("GSE_") );
                    existingIds.addAll( rnaSeqDao.getGeoIds("GSE__") );
                    existingIds.addAll( rnaSeqDao.getGeoIds("GSE___") );

                } else {
                    existingIds = new HashSet<>(rnaSeqDao.getGeoIds("GSE" + indexForFolder + "___"));
                }
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

                    Series series = softFileParser.parse(softFileName, loggerSummary);
                    if (series != null)
                        rnaSeqDao.insertRnaSeq(series);
                    else
                        loggerSummary.error("Parse error : " + softFileName);

                    // remove the file after use (*soft* files take up *a lot* of disk space)
                    new File(softFileName).delete();

                    loggerSummary.info("Updated: "+series.getGeoAccessionID());
                }
            }
            loggerSummary.info("Loaded for folder " + directoryName+ " : "+ loaded.size());
        }
        loggerSummary.info("DownloaderThread-" + threadNum + " => finished, time: " + Calendar.getInstance().getTime());
    }
}
