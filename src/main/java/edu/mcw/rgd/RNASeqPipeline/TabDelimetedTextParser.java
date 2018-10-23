package edu.mcw.rgd.RNASeqPipeline;

/**
 * Created by cdursun on 3/15/2017.
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;

public class TabDelimetedTextParser {
    public static final String DATA_DIRECTORY = "data" + System.getProperty("file.separator");

    private String rnaSeqRgdStrainMappingFileName;
    private byte columnNoForRnaSeqStrainName;
    private byte columnNoForRgdOntTermAccId;
    private byte columnNoForRgdId;


    public Map<String, String> getRnaSeqAndRgdStrainMap(String mapType) throws Exception{
        BufferedReader buf;
        try {
            buf = new BufferedReader(new FileReader(DATA_DIRECTORY + rnaSeqRgdStrainMappingFileName));
        }
        catch (FileNotFoundException e){
            //if there is no rnaSeqRgdStrainMappingFileName then do nothing
            return null;
        }

        Map<String, String> rnaSeqRgdStrainPairMap = new TreeMap<String, String>();
        String lineJustFetched = null;
        String[] wordsArray;
        String rnaSeqStrainName = "", rgdOntTermAccId = "", rgdId = "";
        int i;

        buf.readLine(); //skip the header row
        while(true){
            lineJustFetched = buf.readLine();
            if(lineJustFetched == null){
                break;
            }else{
                wordsArray = lineJustFetched.split("\t");
                i = 0;
                for(String each : wordsArray){
                    if(!"".equals(each)) {
                        if (i == getColumnNoForRnaSeqStrainName()) {
                            rnaSeqStrainName = each;
                        } else if (i == getColumnNoForRgdOntTermAccId()) {
                            if (each.equals("None"))
                                rgdOntTermAccId = null;
                            else
                                rgdOntTermAccId = each;
                        } else if (i == getColumnNoForRgdId()) {
                            if (each.equals("None"))
                                rgdId = null;
                            else
                                rgdId = each;
                        }
                    }
                    i++;
                }


            }
            if (mapType.equals("byRgdId"))
                rnaSeqRgdStrainPairMap.put(rnaSeqStrainName, rgdId);
            else
                rnaSeqRgdStrainPairMap.put(rnaSeqStrainName, rgdOntTermAccId);
        }
        buf.close();
        return rnaSeqRgdStrainPairMap;
    }

    public byte getColumnNoForRnaSeqStrainName() {
        return columnNoForRnaSeqStrainName;
    }

    public void setColumnNoForRnaSeqStrainName(byte columnNoForRnaSeqStrainName) {
        this.columnNoForRnaSeqStrainName = columnNoForRnaSeqStrainName;
    }


    public byte getColumnNoForRgdOntTermAccId() {
        return columnNoForRgdOntTermAccId;
    }

    public void setColumnNoForRgdOntTermAccId(byte columnNoForRgdOntTermAccId) {
        this.columnNoForRgdOntTermAccId = columnNoForRgdOntTermAccId;
    }

    public String getRnaSeqRgdStrainMappingFileName() {
        return rnaSeqRgdStrainMappingFileName;
    }

    public void setRnaSeqRgdStrainMappingFileName(String rnaSeqRgdStrainMappingFileName) {
        this.rnaSeqRgdStrainMappingFileName = rnaSeqRgdStrainMappingFileName;
    }

    public byte getColumnNoForRgdId() {
        return columnNoForRgdId;
    }

    public void setColumnNoForRgdId(byte columnNoForRgdId) {
        this.columnNoForRgdId = columnNoForRgdId;
    }
}
