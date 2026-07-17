package edu.mcw.rgd.RNASeqPipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;

/**
 * Downloads and loads GEO series SOFT files, one NCBI grouping folder at a time (single-threaded).
 */
public class SoftFileLoader {
    private final static Logger loggerSummary = LogManager.getLogger("summary");

    private final RnaSeqDAO rnaSeqDao;
    private final SoftFileDownloader softFileDownloader;
    private final SoftFileParser softFileParser;

    public SoftFileLoader(SoftFileDownloader softFileDownloader, SoftFileParser softFileParser, RnaSeqDAO rnaSeqDao){
        this.rnaSeqDao = rnaSeqDao;
        this.softFileDownloader = softFileDownloader;
        this.softFileParser = softFileParser;
    }

    /**
     * download and load a single GEO series by accession, bypassing the folder scan.
     * The grouping folder is derived from the accession (e.g. GSE53960 -> GSE53nnn/). Unlike
     * processFolder(), the load is always attempted even if the series is already present --
     * duplicate rows are detected and skipped by RnaSeqDAO.insertRnaSeq().
     */
    public void processSingleSeries(String gseAccId) throws Exception {

        String directoryName = SoftFileDownloader.getDirectoryForAccession(gseAccId);
        loggerSummary.info("Single-series load of " + gseAccId + " from folder " + directoryName);

        String softFileName = softFileDownloader.downloadAndExtractSoftFile(directoryName, gseAccId);
        if( softFileName == null ) {
            loggerSummary.error("Download failed or series not found : " + gseAccId);
            return;
        }
        System.out.println(softFileName);

        Series series = softFileParser.parse(softFileName, loggerSummary);

        // remove the file after use (*soft* files take up *a lot* of disk space)
        new File(softFileName).delete();

        if( series == null ) {
            loggerSummary.error("Parse error : " + softFileName);
            return;
        }
        rnaSeqDao.insertRnaSeq(series);
        loggerSummary.info("Updated: " + series.getGeoAccessionID());
    }

    /** download and load every GEO series in one NCBI grouping folder (e.g. GSE44nnn) */
    public void processFolder(int indexForFolder) throws Exception {

        loggerSummary.info("Processing folder: " + indexForFolder + ", time: " + Calendar.getInstance().getTime());

        String directoryName = SoftFileDownloader.getNcbiDirectoryName(indexForFolder);
        softFileDownloader.setExternalFile(SoftFileDownloader.getGeoSoftFilesFtpLink() + directoryName);

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
            // log the actual cause so a transient blip is distinguishable from a real failure
            loggerSummary.error("Directory list error : Skipping directory " + softFileDownloader.getExternalFile(), e);
            return;
        }

        for (String fileAccId : fileAccIds) {
            if(!existingIds.contains(fileAccId)) {
                loaded.add(fileAccId);
                String softFileName = softFileDownloader.downloadAndExtractSoftFile(directoryName, fileAccId);
                if (softFileName == null) continue;
                System.out.println(softFileName);

                Series series = softFileParser.parse(softFileName, loggerSummary);

                // remove the file after use (*soft* files take up *a lot* of disk space)
                new File(softFileName).delete();

                if (series == null) {
                    loggerSummary.error("Parse error : " + softFileName);
                    continue;
                }
                rnaSeqDao.insertRnaSeq(series);
                loggerSummary.info("Updated: " + series.getGeoAccessionID());
            }
        }
        loggerSummary.info("Loaded for folder " + directoryName+ " : "+ loaded.size());
    }
}
