package edu.mcw.rgd.RNASeqPipeline;

import java.util.ArrayList;
import java.util.Map;

import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PubMedLoader {

    private static final Logger logPubmed = LogManager.getLogger("pubmed");

    public static void main(String[] args) throws Exception {

        RnaSeqDAO dao = new RnaSeqDAO();
        Map<String,String> gseToPubmedMap = dao.getPubmedIdsForAllGseAccessions();
        logPubmed.info("processing GSE accessions: "+gseToPubmedMap.size());

        int pubmedIdsChanged = 0;
        int i = 0;
        for( Map.Entry<String,String> entry: gseToPubmedMap.entrySet() ) {

            String gseAccession = entry.getKey();
            String pubmedIdInDb = entry.getValue();

            ArrayList<String> pubmedIdListInGEO = getPubMedIds(gseAccession);
            String pubmedIdsInGEO = Utils.concatenate(pubmedIdListInGEO, ",");

            if( !pubmedIdInDb.equals(pubmedIdsInGEO) ) {
                pubmedIdsChanged++;
                logPubmed.info(gseAccession+": OLD_PUBMED_IDS:"+pubmedIdInDb+",   NEW_PUBMED_IDS:"+pubmedIdsInGEO);
            }

            System.out.println("... "+(++i)+"/"+gseToPubmedMap.size());
        }

        logPubmed.info("OK! pubmed ids changed: "+pubmedIdsChanged);
    }

    public static ArrayList<String> getPubMedIds(String gseAccession) throws Exception {

        String url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?view=brief&acc="+gseAccession;
        FileDownloader fd = new FileDownloader();
        fd.setExternalFile(url);
        String content = fd.download();

        ArrayList<String> pubmedIds = new ArrayList<>();

        int pos = 0;
        while( pos >=0 ) {

            // look for all fragments like this one:
            // <span class="pubmed_id" id="22010005">
            String searchStr = "<span class=\"pubmed_id\" id=\"";
            int pmidPos = content.indexOf(searchStr, pos);
            if( pmidPos<0 ) {
                break;
            }
            pmidPos += searchStr.length();
            int pmidPos2 = content.indexOf('\"', pmidPos);
            if( pmidPos2 > pmidPos ) {
                String pmid = content.substring(pmidPos, pmidPos2);
                pubmedIds.add(pmid);
            }

            pos = pmidPos2;
        }
        return pubmedIds;
    }
}