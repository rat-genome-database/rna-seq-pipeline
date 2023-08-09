package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;

/**
 * Created by cdursun on 5/10/2017.
 */
public class SoftFileDownloader extends FileDownloader {
    private static final String DATA_DIRECTORY = "data/";
    private static final String SOFT_FILE_PREFIX = "GSE";
    private static final String SOFT_FILE_SUFFIX = "_family.soft.gz";
    private static String NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK;

    private CounterPool counters = new CounterPool();

    private final static Logger loggerDownloaded = LogManager.getLogger("downloaded");
    private final static Logger loggerEmpty = LogManager.getLogger("empty");
    private final static Logger loggerRgd = LogManager.getLogger("log_rgd");

    public SoftFileDownloader(byte maxRetryCount, byte downloadRetryIntervalInSeconds, CounterPool counters){
        this.setMaxRetryCount(maxRetryCount);
        this.setDownloadRetryInterval(downloadRetryIntervalInSeconds);
        this.setUseCompression(false);
        this.counters = counters;
    }

    public String downloadAndExtractSoftFile(String directory, String gseAccId) {
        String localFile = downloadSoftFile(directory, gseAccId);
        return localFile;
    }

    public String downloadSoftFile(String directory, String gseAccId) {

        String externalFileName = NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK + directory
                + gseAccId + "/soft/" + gseAccId   + SOFT_FILE_SUFFIX;

        setExternalFile(externalFileName);
        setLocalFile(DATA_DIRECTORY + gseAccId  + SOFT_FILE_SUFFIX);
        try {
            String localFile = download();
            loggerDownloaded.info("downloaded: "+localFile);
            counters.increment("numberOfDownloadedFiles");
            return localFile;

        }catch (PermanentDownloadErrorException e){
            //just skip
        }
        catch (Exception e){
            loggerDownloaded.error("SoftFileDownloader.downloadSoftFile() : " + getLocalFile() + " - " + e);
        }
        return null;
    }

    /**
     * list files for the current working directory
     * @return list of file names
     * @throws Exception when unexpected things happen
     */
    public String[] listFiles() throws Exception {
        loggerRgd.info("Listing contents of " + this.getExternalFile());

        // we must break the url into server part and the rest
        int slashPos = this.getExternalFile().indexOf('/', 6); // look for '/' pos skipping initial 'ftp://'
        if( slashPos<0 )
            throw new Exception("malformed ftp url");

        String ftpServer = this.getExternalFile().substring(6, slashPos);
        String ftpFile = this.getExternalFile().substring(slashPos);

        FTPClient client = new FTPClient();

        try {
            // try to connect and log-in as anonymous to ftp server
            doFtpConnect(client, ftpServer);
            client.changeWorkingDirectory(this.getExternalFile().substring(slashPos));

            // get list of directories
            FTPFile[] ftpFiles = client.listDirectories();
            String[] fileNames = new String[ftpFiles.length];
            for( int i=0; i<ftpFiles.length; i++ ) {
                fileNames[i] = ftpFiles[i].getName();
            }

            // return the list of file names
            return fileNames;
        }
        finally {
            try {
                client.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
                loggerRgd.debug("ftp close/disconnect: "+e.getMessage()+" "+e.toString());
            }
        }
    }

    public static void setGeoSoftFilesFtpLink(String ftpLink) {
        NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK = ftpLink;
    }

    public static String getGeoSoftFilesFtpLink() {
        return NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK;
    }

    public static String getNcbiDirectoryName(int i){
        return SOFT_FILE_PREFIX + (i == 0 ? "" : i) +  "nnn/";
    }

}
