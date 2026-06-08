package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cdursun on 5/10/2017.
 * <p>
 * Downloads GEO series SOFT files from NCBI over HTTP(S). NCBI serves the GEO ftp tree over https
 * (e.g. https://ftp.ncbi.nlm.nih.gov/geo/series/), so both the file download and the directory
 * listing use HTTP -- the listing parses the server's autoindex HTML.
 */
public class SoftFileDownloader extends FileDownloader2 {
    private static final String DATA_DIRECTORY = "data/";
    private static final String SOFT_FILE_PREFIX = "GSE";
    private static final String SOFT_FILE_SUFFIX = "_family.soft.gz";
    private static String NCBI_GEO_SERIES_SOFT_FILES_FTP_LINK;

    // autoindex entry for a GEO series sub-directory, e.g. <a href="GSE123/">GSE123/</a>
    private static final Pattern DIR_LINK = Pattern.compile("href=\"(GSE\\d+)/\"", Pattern.CASE_INSENSITIVE);

    private final CounterPool counters;

    private final static Logger loggerDownloaded = LogManager.getLogger("downloaded");
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
     * list the GEO series sub-directories for the current external url (the grouping folder),
     * by reading the server's autoindex page over HTTP and parsing the directory links
     * @return list of GSE accession directory names (f.e. GSE1, GSE10, ...)
     * @throws Exception when the listing cannot be retrieved
     */
    public String[] listFiles() throws Exception {
        String url = this.getExternalFile();
        loggerRgd.info("Listing contents of " + url);

        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(60))
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(120))
                .header("User-Agent", "RGD-rna-seq-pipeline")
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if( response.statusCode() != 200 ) {
            throw new Exception("listing " + url + " returned HTTP " + response.statusCode());
        }

        List<String> fileNames = new ArrayList<>();
        Matcher m = DIR_LINK.matcher(response.body());
        while( m.find() ) {
            fileNames.add(m.group(1));
        }
        return fileNames.toArray(new String[0]);
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
