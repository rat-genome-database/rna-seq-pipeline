package edu.mcw.rgd.RNASeqPipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Calendar;

/**
 * Created by cdursun on 7/21/2017.
 */
public class MapperThread implements Runnable {
    private final static Logger loggerSummary = LogManager.getLogger("summary");
    private int threadNum;
    private int startIndex;
    private int stopIndex;
    private RnaSeqToRgdMapper rnaSeqToRgdMapper;

    public MapperThread(int threadNum, RnaSeqToRgdMapper rnaSeqToRgdMapper, int startIndex, int stopIndex){
        this.threadNum = threadNum;
        this.startIndex = startIndex;
        this.stopIndex = stopIndex;
        this.rnaSeqToRgdMapper = rnaSeqToRgdMapper;
    }
    public void run() {
        loggerSummary.info("MapperThread-" + threadNum + " => started interval: " + startIndex + "-" + stopIndex + " time: " + Calendar.getInstance().getTime());
        for (int j = startIndex; j <stopIndex ; j++) {
            rnaSeqToRgdMapper.mapRnaSeqToRgd(rnaSeqToRgdMapper.getRnaSeqList().get(j));
            if ((j-startIndex) != 0 && (j-startIndex) % rnaSeqToRgdMapper.getDbConnectionCheckInterval() == 0) {
                loggerSummary.info("MapperThread-" + threadNum + " => processed record: " + j + ", conn validity: " + rnaSeqToRgdMapper.checkDbConnection());
            }
        }
        loggerSummary.info("MapperThread-" + threadNum + " => finished interval: " + startIndex + "-" + stopIndex + " time: " + Calendar.getInstance().getTime());
    }
}
