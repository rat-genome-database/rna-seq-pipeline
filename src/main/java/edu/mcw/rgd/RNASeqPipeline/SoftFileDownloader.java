package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.process.FileDownloader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by cdursun on 5/10/2017.
 */
public class SoftFileDownloader extends FileDownloader {
    private static final String DATA_DIRECTORY = "data/";
    private static final String SOFT_FILE_PREFIX = "GSE";
    private static final String SOFT_FILE_SUFFIX = "_family.soft.gz";
    private static String NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK;

    private int numberOfEmptyFiles = 0;
    private int numberOfDownloadedFiles = 0;
    private final static Log loggerDownloaded;
    private final static Log loggerEmpty;
    private final static Log loggerRgd;
    static {
        loggerDownloaded = LogFactory.getLog("downloaded");
        loggerEmpty = LogFactory.getLog("empty");
        loggerRgd = LogFactory.getLog("log_rgd");
    }

    public SoftFileDownloader(byte maxRetryCount, byte downloadRetryIntervalInSeconds){
        this.setMaxRetryCount(maxRetryCount);
        this.setDownloadRetryInterval(downloadRetryIntervalInSeconds);
        this.setUseCompression(false);
    }

    /*public String downloadAndExtractSoftFile(int i, int j) {
        downloadSoftFile(i, j);
        return extractDownlodedGzFile();
    }*/
    public String downloadAndExtractSoftFile(String directory, String gseAccId) {
        downloadSoftFile(directory, gseAccId);
        return extractDownlodedGzFile();
    }

   /* public void downloadSoftFile(int i, int j) {

        String GSEFileName = SOFT_FILE_PREFIX + (i * 1000 + j);
        String externalFileName = NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK + getNcbiDirectoryName(i)
                        + GSEFileName + "/soft/" + GSEFileName   + SOFT_FILE_SUFFIX;

        setExternalFile(externalFileName);
        setLocalFile(DATA_DIRECTORY + GSEFileName  + SOFT_FILE_SUFFIX);
        try {
            download();
            loggerDownloaded.info(GSEFileName + SOFT_FILE_SUFFIX);
            synchronized (this) {
                numberOfDownloadedFiles++;
            }
        }catch (PermanentDownloadErrorException e){
            //just skip
        }
        catch (Exception e){
            loggerDownloaded.error("SoftFileDownloader.downloadSoftFile() : " + getLocalFile() + " - " + e);
        }
    }*/

    public void downloadSoftFile(String directory, String gseAccId) {

        String externalFileName = NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK + directory
                + gseAccId + "/soft/" + gseAccId   + SOFT_FILE_SUFFIX;

        setExternalFile(externalFileName);
        setLocalFile(DATA_DIRECTORY + gseAccId  + SOFT_FILE_SUFFIX);
        try {
            download();
            loggerDownloaded.info(gseAccId + SOFT_FILE_SUFFIX);
            synchronized (this) {
                numberOfDownloadedFiles++;
            }
        }catch (PermanentDownloadErrorException e){
            //just skip
        }
        catch (Exception e){
            loggerDownloaded.error("SoftFileDownloader.downloadSoftFile() : " + getLocalFile() + " - " + e);
        }
    }

    public String extractDownlodedGzFile() {
        String softFileName = getLocalFile().substring(0, getLocalFile().indexOf(".gz"));
        File f = new File(getLocalFile());
        if (f.length() == 0){
            loggerEmpty.info(f.getName());
            f.delete();
            return null;
        }
        else{
            gunzipFile(getLocalFile(), softFileName);
            f.delete();
        }
        return softFileName;
    }
/*    public void deleteEmptyFiles(){

        File[] localFileList = listLocalFiles(DATA_DIRECTORY, SOFT_FILE_PREFIX);

        if (localFileList == null)
            return;

        for (int i = 0; i < localFileList.length; i++) {
            if ( localFileList[i].length() == 0) {
                loggerEmpty.info(localFileList[i].getName());
                localFileList[i].delete();
                numberOfEmptyFiles++;
            }
        }
    }*/

    /*public static File[] listLocalFiles(String fileDirectory, final String fileName){
        return new File(fileDirectory).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.matches(fileName + "(.*)");
            }
        });
    }*/

  /*  public void gunzipAllFiles(){
        File[] localFileList = listLocalFiles(DATA_DIRECTORY, SOFT_FILE_PREFIX);

        if (localFileList == null)
            return;

        for (int i = 0; i < localFileList.length; i++) {
            gunzipFile(DATA_DIRECTORY + localFileList[i].getName(),
                    DATA_DIRECTORY + localFileList[i].getName().substring(0, localFileList[i].getName().indexOf(".gz")));
            localFileList[i].delete();
        }
    }*/

    public void gunzipFile(String gzipFile, String softFile){

        byte[] buffer = new byte[1024];

        try{

            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(gzipFile));

            FileOutputStream out = new FileOutputStream(softFile);

            int len;
            while ((len = gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }

            gzis.close();
            out.close();

        }catch(IOException e){
            loggerDownloaded.error("SoftFileDownloader.gunzipFile() : " + gzipFile + " - " + e );
        }
    }
   /* public String[] listFiles2() throws Exception {
        System.out.println("Listing contents of " + this.getExternalFile());
        int slashPos = this.getExternalFile().indexOf('/', 6);
        if(slashPos < 0) {
            throw new Exception("malformed ftp url");
        } else {
            String ftpServer = this.getExternalFile().substring(6, slashPos);
            //String ftpServer = this.externalFile.substring(6);
            System.out.println("FTP SERVER :: " + ftpServer);
            String ftpFile = this.getExternalFile().substring(slashPos);
            FTPClient client = new FTPClient();
            //System.out.println("====> " + this.externalFile.substring(slashPos + 1));


            try {
                this.doFtpConnect(client, ftpServer);
                client.setSoTimeout(20000);
                client.setConnectTimeout(20000);
                client.changeWorkingDirectory(this.getExternalFile().substring(slashPos));
                FTPFile[] ftpFiles = client.listDirectories();
                String[] fileNames = new String[ftpFiles.length];

                for(int i = 0; i < ftpFiles.length; ++i) {
                    fileNames[i] = ftpFiles[i].getName();
                }

                String[] var16 = fileNames;
                return var16;
            } finally {
                try {
                    client.disconnect();
                } catch (IOException var14) {
                    var14.printStackTrace();
                    System.out.println("ftp close/disconnect: " + var14.getMessage() + " " + var14.toString());
                }

            }
        }
    }*/

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

    public int getNumberOfDownloadedFiles() {
        return numberOfDownloadedFiles;
    }
    public int getNumberOfEmptyFiles() {
        return numberOfEmptyFiles;
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
