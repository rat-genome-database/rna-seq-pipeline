package edu.mcw.rgd.RNASeqPipeline;

import java.io.*;

// Assume that there is only ONE instance of this class 
// at any given time, so much of the content is static
public class SoftFileParser {
    static final boolean DEBUG = false;

    private String version;

    public static String getCurrentLocation() {
        return new File("").getAbsolutePath() + "/data";
    }

    public static File[] getFileList(String location) {
        //return new File(location).listFiles();
        return new File(location).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".soft");
            }
        });
    }

    public void readFileList(File[] listOfFiles) {
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].toString().endsWith(".soft")) {
                if (DEBUG) System.out.println("\nSOFT file to read: " + listOfFiles[i] + "\n-----------------");

                parse(listOfFiles[i]);
            }
        }
    }
    public Series parse(File inputFile) {
        Series series = null;
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            series = new Series();
            for (String line; (line = br.readLine()) != null; ) {
                if (line.startsWith("^SERIES") || line.startsWith("!Series")) {
                    series.handleSeries(line);
                }
                else if (line.startsWith("^PLATFORM") || line.startsWith("!Platform")) {
                    if (line.startsWith("^PLATFORM")){
                        series.getPlatformList().add(new Platform());
                    }
                    series.getLastPlatform().handlePlatform(line);
                }
                else if (line.startsWith("^SAMPLE") || line.startsWith("!Sample")){
                    if (line.startsWith("^SAMPLE")){
                        series.getSampleList().add(new Sample());
                    }
                    series.getLastSample().handleSample(line);
                }

                //handleSOFTobjects(series, line);
            }
            return series;
        } catch (IOException error) {
            System.err.println("Caught IOException: " + error);
        }
        return null;
    }




    public static void main(String[] args) {
        // Print-out current location
        if (DEBUG) System.out.println("Current working location: " + getCurrentLocation());

        // Get the list of files from the local directory
        File[] listOfFiles = getFileList(getCurrentLocation());

        // Read the local *.SOFT files
        //readFileList(listOfFiles);

        // Print basic Series information
        // Series.printOnlySeries();

        // Print selected Series and Sample elements
       // Series.printSeriesAndSamples("out.tsv");
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

}
