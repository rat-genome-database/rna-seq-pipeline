package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.dao.impl.StrainDAO;
import edu.mcw.rgd.datamodel.Strain;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Created by cdursun on 7/7/2017.
 */
public class RnaSeqToRgdMapper{

    private final static Logger loggerSummary = LogManager.getLogger("summary");
    private String crossSpeciesAnatomyOntId;
    private String ontTermExactSynonymType;

    private List<RnaSeq> rnaSeqList;
    //private Set<RnaSeq> rnaSeqListToBeMappedByOntTerm = new HashSet<RnaSeq>();
    //private Set<RnaSeq> rnaSeqListToBeMappedByStrain = new HashSet<RnaSeq>();
    private static Map<String, String> rnaSeqRgdStrainPairMapByTermAcc;
    private static Map<String, String> rnaSeqRgdStrainPairMapByRgdId;

    private static List<Term> crossSpeciesTerms;
    private static List<TermSynonym> crossSpeciesSynoyms;
    private static List<Term> cellOntTerms;
    private static List<TermSynonym> cellOntSynoyms;
    private static List<Term> ratStrainOntTerms;
    private static List<TermSynonym> ratStrainOntSynoyms ;
    private static List<Strain> strains;
    private static LemmatizerDragon lemmatizerDragon;
    private RnaSeqDAO rnaSeqDao = new RnaSeqDAO();

    private String cellOntId;
    private String ratStrainsOntId;

    private TabDelimetedTextParser tabDelimetedTextParser;
    private int numberOfMappingsAfterLemmatization = 0;

    private int dbConnectionCheckInterval;


    public void init(Date dateCutoff) throws Exception{


        StrainDAO strainDAO = new StrainDAO();
        OntologyXDAO ontologyXDAO = new OntologyXDAO();

        rnaSeqList = rnaSeqDao.getAllRnaSeq(dateCutoff);
        rnaSeqRgdStrainPairMapByTermAcc = tabDelimetedTextParser.getRnaSeqAndRgdStrainMap("byOntTermAccId");
        rnaSeqRgdStrainPairMapByRgdId = tabDelimetedTextParser.getRnaSeqAndRgdStrainMap("byRgdId");
        crossSpeciesTerms = ontologyXDAO.getActiveTerms(crossSpeciesAnatomyOntId);
        crossSpeciesSynoyms = ontologyXDAO.getActiveSynonymsByType(crossSpeciesAnatomyOntId, ontTermExactSynonymType);
        cellOntTerms = ontologyXDAO.getActiveTerms(cellOntId);
        cellOntSynoyms = ontologyXDAO.getActiveSynonymsByType(cellOntId, ontTermExactSynonymType);
        ratStrainOntTerms = ontologyXDAO.getActiveTerms(ratStrainsOntId);
        ratStrainOntSynoyms = ontologyXDAO.getActiveSynonymsByType(ratStrainsOntId, ontTermExactSynonymType);
        strains = strainDAO.getActiveStrains();
        loggerSummary.info("UBERON Terms size : " + crossSpeciesTerms.size());
        loggerSummary.info("UBERON Synonyms size : " + crossSpeciesSynoyms.size());
        loggerSummary.info("CL Terms size : " + cellOntTerms.size());
        loggerSummary.info("CL Synonyms size : " + cellOntSynoyms.size());
        loggerSummary.info("RS Terms size : " + ratStrainOntTerms.size());
        loggerSummary.info("RS Synonyms size : " + ratStrainOntSynoyms.size());
        lemmatizerDragon = LemmatizerDragon.getInstance();

    }

    public String matchRnaSeqStrainToRgdByOntTerm(String sampleStrain){

        if (sampleStrain == null) return null;

        return rnaSeqRgdStrainPairMapByTermAcc.get(sampleStrain);

    }

    public Integer matchRnaSeqStrainToRgdByRgdId(String sampleStrain){

        if (sampleStrain == null) return null;

        String rgdId = rnaSeqRgdStrainPairMapByRgdId.get(sampleStrain);

        if (rgdId == null) return null;

        return Integer.valueOf(rgdId);
    }
    /**
     * Map RnaSeq data by comparing sampleTissue with Ontology Terms
     * comparison is done by both Term comparison and Exact_synonym comparison from Synonyms
     */
    public void mapRnaSeqToRgd(RnaSeq rnaSeq){
        try {

            boolean isMappedBySampleTissueTerm = mapByOntTerm(rnaSeq.getSampleTissue(), rnaSeq, crossSpeciesTerms, false);
            boolean isMappedBySampleCellTerm = mapByOntTerm(rnaSeq.getSampleCellLine(), rnaSeq, cellOntTerms, false);

            if (!isMappedBySampleCellTerm)
                isMappedBySampleCellTerm = mapByOntTerm(rnaSeq.getSampleCellType(), rnaSeq, cellOntTerms, false);

            boolean isMappedBySampleStrainTerm = mapByOntTerm(rnaSeq.getSampleStrain(), rnaSeq, ratStrainOntTerms, false);

            if (!isMappedBySampleTissueTerm)
                isMappedBySampleTissueTerm = mapByOntSynoym(rnaSeq.getSampleTissue(), rnaSeq, crossSpeciesSynoyms, crossSpeciesAnatomyOntId, false);

            if (!isMappedBySampleCellTerm)
                isMappedBySampleCellTerm = mapByOntSynoym(rnaSeq.getSampleCellLine(), rnaSeq, cellOntSynoyms, cellOntId, false);

            if (!isMappedBySampleCellTerm)
                isMappedBySampleCellTerm = mapByOntSynoym(rnaSeq.getSampleCellType(), rnaSeq, cellOntSynoyms, cellOntId, false);

            if (!isMappedBySampleStrainTerm)
                isMappedBySampleStrainTerm = mapByOntSynoym(rnaSeq.getSampleStrain(), rnaSeq, ratStrainOntSynoyms, ratStrainsOntId, false);

            boolean isMappedBySampleStrainRgdId = mapByStrain(rnaSeq.getSampleStrain(), rnaSeq, strains, false);

            boolean lemResultBySampleTissueTerm = false;
            boolean lemResultBySampleCellTerm = false;
            boolean lemResultBySampleStrainTerm = false;
            boolean lemResultBySampleStrainRgdId = false;


            if (!isMappedBySampleTissueTerm){
                lemResultBySampleTissueTerm = mapByOntTerm(lemmatize(rnaSeq.getSampleTissue()), rnaSeq, crossSpeciesTerms, true);

                if (!lemResultBySampleTissueTerm)
                    lemResultBySampleTissueTerm = mapByOntSynoym(lemmatize(rnaSeq.getSampleTissue()), rnaSeq, crossSpeciesSynoyms, crossSpeciesAnatomyOntId, true);

            }

            if (!isMappedBySampleCellTerm) {
                lemResultBySampleCellTerm = mapByOntTerm(lemmatize(rnaSeq.getSampleCellLine()), rnaSeq, cellOntTerms, true);

                if (!lemResultBySampleCellTerm)
                    lemResultBySampleCellTerm = mapByOntTerm(lemmatize(rnaSeq.getSampleCellType()), rnaSeq, cellOntTerms, true);

                if (!lemResultBySampleCellTerm)
                    lemResultBySampleCellTerm = mapByOntSynoym(lemmatize(rnaSeq.getSampleCellLine()), rnaSeq, cellOntSynoyms, cellOntId, true);

                if (!lemResultBySampleCellTerm)
                    lemResultBySampleCellTerm = mapByOntSynoym(lemmatize(rnaSeq.getSampleCellType()), rnaSeq, cellOntSynoyms, cellOntId, true);
            }


            if (!isMappedBySampleStrainTerm) {
                lemResultBySampleStrainTerm = mapByOntTerm(lemmatize(rnaSeq.getSampleStrain()), rnaSeq, ratStrainOntTerms, true);

                if (!lemResultBySampleStrainTerm)
                    lemResultBySampleStrainTerm = mapByOntSynoym(lemmatize(rnaSeq.getSampleStrain()), rnaSeq, ratStrainOntSynoyms, ratStrainsOntId, true);

            }

            if (!isMappedBySampleStrainRgdId)
                lemResultBySampleStrainRgdId = mapByStrain(lemmatize(rnaSeq.getSampleStrain()), rnaSeq, strains, true);


            if (lemResultBySampleTissueTerm || lemResultBySampleCellTerm || lemResultBySampleStrainTerm || lemResultBySampleStrainRgdId) {
                //System.out.println(rnaSeq.getKey() + " - " + lemResultBySampleTissueTerm + " - " + lemResultBySampleCellTerm  + " - " +  lemResultBySampleStrainTerm  + " - " +  lemResultBySampleStrainRgdId);
                synchronized (this) {
                    numberOfMappingsAfterLemmatization++;
                }
            }


            rnaSeqDao.updateRgdFields(rnaSeq);

            loggerSummary.debug("  MAPPED key="+rnaSeq.getKey()+", numberOfMappingsAfterLemmatization="+numberOfMappingsAfterLemmatization);
        }
        catch (Exception e){
            loggerSummary.info(this.getClass() + " - mapRnaSeqToRgd ->" + rnaSeq.getKey() + "- " + e);
        }
     }

    public int checkDbConnection(){
        int status = 0;
        try {
            status = rnaSeqDao.checkConnection();
        } catch (Exception e) {
            loggerSummary.error(this.getClass() + " - checkDbConnection ->"  + e);
        }
        return status;
    }

    public void applyMappingToDb() throws Exception{
        /*loggerSummary.info("Total number of records by OntTerm : " + rnaSeqListToBeMappedByOntTerm.size());
        rnaSeqDao.updateRgdOntTermAccs(rnaSeqListToBeMappedByOntTerm);

        loggerSummary.info("Total number of records by Strains : " + rnaSeqListToBeMappedByStrain.size());
        rnaSeqDao.updateRgdStrainRgdIds(rnaSeqListToBeMappedByStrain);*/
        rnaSeqDao.updateRgdMappingFields(rnaSeqList);
        loggerSummary.info("Total number of records after Lemmatization : " + numberOfMappingsAfterLemmatization);
    }

    public String lemmatize(String input) throws Exception {
        if (input == null) return null;
        return lemmatizerDragon.LemmaSent(Stopwords.removeStopWords(input));
    }




  /*  private byte mapByOntTermOrSynonym(String rnaSeqString, RnaSeq rnaSeq, List<Term>  terms ) throws Exception{

        if(rnaSeqString == null) return 0;

        for (Term term : terms) {
            if (rnaSeqString.toLowerCase().equals(term.getTerm().toLowerCase())) {
                rnaSeq.setRgdOntTermAcc(term.getAccId());
                rnaSeqDao.updateRgdOntTermAcc(rnaSeq);
                loggerSummary.info("updated by term");
                return 1;
            } else{
                List<TermSynonym> synonyms = ontologyXDAO.getTermSynonyms(term.getAccId());
                for (TermSynonym synonym : synonyms) {
                    if (synonym.getType().equals(ontTermExactSynonymType) && rnaSeqString.toLowerCase().equals(synonym.getName().toLowerCase())){
                        rnaSeq.setRgdOntTermAcc(term.getAccId());
                        rnaSeqDao.updateRgdOntTermAcc(rnaSeq);
                        loggerSummary.info("updated by synonym");
                        return 1;
                    }
                }
            }
        }
        return 0;
    }*/

    public boolean mapByOntTerm(String rnaSeqString, RnaSeq rnaSeq, List<Term>  terms, boolean lem ) throws Exception{
        if(rnaSeqString == null || rnaSeqString.equals("")) return false;
        for (Term term : terms) {
            String t = term.getTerm();
            if (lem) t = lemmatize(t);
            else if (term.getOntologyId().equals(ratStrainsOntId)){
                String termAcc = matchRnaSeqStrainToRgdByOntTerm(rnaSeqString);
                if (termAcc != null){
                    rnaSeq.setRgdStrainTermAcc(term.getAccId());
                    /*synchronized(this) {
                        rnaSeqListToBeMappedByOntTerm.add(rnaSeq);
                    }*/
                    return true;
                }
            }
            else{
                t = t.toLowerCase();
                rnaSeqString = rnaSeqString.toLowerCase();
            }

            if ( rnaSeqString.equals(t) ) {
                if (term.getOntologyId().equals(crossSpeciesAnatomyOntId))
                    rnaSeq.setRgdTissueTermAcc(term.getAccId());
                else if (term.getOntologyId().equals(cellOntId))
                    rnaSeq.setRgdCellTermAcc(term.getAccId());
                else if (term.getOntologyId().equals(ratStrainsOntId))
                    rnaSeq.setRgdStrainTermAcc(term.getAccId());

               /*synchronized(this) {
                   rnaSeqListToBeMappedByOntTerm.add(rnaSeq);
               }*/
               return true;
            }
        }
        return false;
    }

    public boolean mapByOntSynoym(String rnaSeqString, RnaSeq rnaSeq, List<TermSynonym>  synonyms, String termOntId, boolean lem) throws Exception{
        if(rnaSeqString == null || rnaSeqString.equals("")) return false;
        for (TermSynonym synonym : synonyms) {
            String s = synonym.getName();
            if (lem) s = lemmatize(s);
            else{
                s = s.toLowerCase();
                rnaSeqString = rnaSeqString.toLowerCase();
            }

            if ( rnaSeqString.equals(s) ){
                if (termOntId.equals(crossSpeciesAnatomyOntId))
                    rnaSeq.setRgdTissueTermAcc(synonym.getTermAcc());
                else if (termOntId.equals(cellOntId))
                    rnaSeq.setRgdCellTermAcc(synonym.getTermAcc());
                else if (termOntId.equals(ratStrainsOntId))
                    rnaSeq.setRgdStrainTermAcc(synonym.getTermAcc());

                /*synchronized(this) {
                    rnaSeqListToBeMappedByOntTerm.add(rnaSeq);
                }*/

                return true;
            }
        }
        return false;
    }

    private boolean mapByStrain(String sampleStrain, RnaSeq rnaSeq, List<Strain> strains, boolean lem) throws Exception {
        Integer rgdId = null;
        if(sampleStrain == null) return false;
        for (Strain strain : strains) {
            String s = strain.getSymbol().toLowerCase();
            if (lem) s = lemmatize(s);
            else if ((rgdId = matchRnaSeqStrainToRgdByRgdId(sampleStrain)) != null){
                rnaSeq.setRgdStrainRgdId(rgdId);
                /*synchronized(this) {
                    rnaSeqListToBeMappedByStrain.add(rnaSeq);
                }*/
                return true;
            }
            else{
                s = s.toLowerCase();
                sampleStrain = sampleStrain.toLowerCase();
            }

            if ( sampleStrain.equals(s)/*|| sampleStrain.toLowerCase().equals(strain.getName().toLowerCase())*/) {
                rnaSeq.setRgdStrainRgdId(strain.getRgdId());
                /*synchronized(this) {
                    rnaSeqListToBeMappedByStrain.add(rnaSeq);
                }*/
                return true;
            }
        }
        return false;
    }

    /**
     * Map RnaSeq data by comparing sampleTissue with Ontology Terms
     * comparison is done by both Term comparison and Exact_synonym comparison from Synonyms
     *
     */
/*    public void mapRnaSeqToRgd() throws Exception {
        RnaSeqDAO rnaSeqDao = new RnaSeqDAO();

        rnaSeqList = rnaSeqDao.getAllRnaSeq();
        rnaSeqRgdStrainPairMap = tabDelimetedTextParser.getRnaSeqAndRgdStrainMap();

        List<Term> crossSpeciesTerms = ontologyXDAO.getActiveTerms(crossSpeciesAnatomyOntId);
        List<TermSynonym> crossSpeciesSynoyms = ontologyXDAO.getActiveSynonymsByType(crossSpeciesAnatomyOntId, ontTermExactSynonymType);
        List<Term> cellOntTerms = ontologyXDAO.getActiveTerms(cellOntId);
        List<TermSynonym> cellOntSynoyms = ontologyXDAO.getActiveSynonymsByType(cellOntId, ontTermExactSynonymType);
        List<Term> ratStrainOntTerms = ontologyXDAO.getActiveTerms(ratStrainsOntId);
        List<TermSynonym> ratStrainOntSynoyms = ontologyXDAO.getActiveSynonymsByType(ratStrainsOntId, ontTermExactSynonymType);
        List<Strain> strains = strainDAO.getActiveStrains();


        loggerSummary.info("UBERON Terms size : " + crossSpeciesTerms.size());
        loggerSummary.info("UBERON Synonyms size : " + crossSpeciesSynoyms.size());
        loggerSummary.info("CL Terms size : " + cellOntTerms.size());
        loggerSummary.info("CL Synonyms size : " + cellOntSynoyms.size());
        loggerSummary.info("RS Terms size : " + ratStrainOntTerms.size());
        loggerSummary.info("RS Synonyms size : " + ratStrainOntSynoyms.size());

        int numberOfMappingsAfterLemmatization = 0;
        int i = 1;
        for(RnaSeq rnaSeq : rnaSeqList) {
            boolean lemResult = false;
            if (!mapByOntTerm(rnaSeq.getSampleTissue(), rnaSeq, crossSpeciesTerms, false))
                if (!mapByOntTerm(rnaSeq.getSampleCellLine(), rnaSeq, cellOntTerms, false))
                    if (!mapByOntTerm(rnaSeq.getSampleStrain(), rnaSeq, ratStrainOntTerms, false))
                        if(!mapByOntSynoym(rnaSeq.getSampleTissue(), rnaSeq, crossSpeciesSynoyms, false))
                            if(!mapByOntSynoym(rnaSeq.getSampleCellLine(), rnaSeq, cellOntSynoyms, false))
                                if(!mapByOntSynoym(rnaSeq.getSampleStrain(), rnaSeq, ratStrainOntSynoyms, false))
                                    if(!mapByStrain(rnaSeq.getSampleStrain(), rnaSeq, strains, false)){
                                        lemResult = true;
                                        if(!mapByOntTerm(lemmatize(rnaSeq.getSampleTissue()), rnaSeq, crossSpeciesTerms, true))
                                            if(!mapByOntTerm(lemmatize(rnaSeq.getSampleCellLine()), rnaSeq, cellOntTerms, true))
                                                if(!mapByOntTerm(lemmatize(rnaSeq.getSampleStrain()), rnaSeq, ratStrainOntTerms, true))
                                                    if(!mapByOntSynoym(lemmatize(rnaSeq.getSampleTissue()), rnaSeq, crossSpeciesSynoyms, true))
                                                        if(!mapByOntSynoym(lemmatize(rnaSeq.getSampleCellLine()), rnaSeq, cellOntSynoyms, true))
                                                            if(!mapByOntSynoym(lemmatize(rnaSeq.getSampleStrain()), rnaSeq, ratStrainOntSynoyms, true))
                                                                if(!mapByStrain(lemmatize(rnaSeq.getSampleStrain()), rnaSeq, strains, true))
                                                                    lemResult = false;
                                    }


            if (i++ % 2000 == 0) {
                loggerSummary.info("processed: " + (i-1) + ", conn validity: " + rnaSeqDao.checkConnection());
                loggerSummary.info("Total number of records by OntTerm : " + rnaSeqListToBeMappedByOntTerm.size());
                loggerSummary.info("Total number of records by Strains : " + rnaSeqListToBeMappedByStrain.size());
                loggerSummary.info("Total number of records after Lemmatization : " + numberOfMappingsAfterLemmatization);
            }

            if (lemResult) numberOfMappingsAfterLemmatization++;
        }

        loggerSummary.info("Total number of records by OntTerm : " + rnaSeqListToBeMappedByOntTerm.size());
        rnaSeqDao.updateRgdOntTermAccs(rnaSeqListToBeMappedByOntTerm);

        loggerSummary.info("Total number of records by Strains : " + rnaSeqListToBeMappedByStrain.size());
        rnaSeqDao.updateRgdStrainRgdIds(rnaSeqListToBeMappedByStrain);

        loggerSummary.info("Total number of records after Lemmatization : " + numberOfMappingsAfterLemmatization);
    }*/


    public void setCrossSpeciesAnatomyOntId(String crossSpeciesAnatomyOntId) {
        this.crossSpeciesAnatomyOntId = crossSpeciesAnatomyOntId;
    }

    public String getCrossSpeciesAnatomyOntId() {
        return crossSpeciesAnatomyOntId;
    }

    public void setOntTermExactSynonymType(String ontTermExactSynonymType) {
        this.ontTermExactSynonymType = ontTermExactSynonymType;
    }

    public String getOntTermExactSynonymType() {
        return ontTermExactSynonymType;
    }

    public void setCellOntId(String cellOntId) {
        this.cellOntId = cellOntId;
    }

    public String getCellOntId() {
        return cellOntId;
    }

    public void setRatStrainsOntId(String ratStrainsOntId) {
        this.ratStrainsOntId = ratStrainsOntId;
    }

    public String getRatStrainsOntId() {
        return ratStrainsOntId;
    }

    public void setTabDelimetedTextParser(TabDelimetedTextParser tabDelimetedTextParser) {
        this.tabDelimetedTextParser = tabDelimetedTextParser;
    }

    public TabDelimetedTextParser getTabDelimetedTextParser() {
        return tabDelimetedTextParser;
    }

    public List<RnaSeq> getRnaSeqList() {
        return rnaSeqList;
    }

    public void setDbConnectionCheckInterval(int dbConnectionCheckInterval) {
        this.dbConnectionCheckInterval = dbConnectionCheckInterval;
    }

    public int getDbConnectionCheckInterval() {
        return dbConnectionCheckInterval;
    }

    public int getNumberOfMappingsAfterLemmatization() {
        return numberOfMappingsAfterLemmatization;
    }

    public void setNumberOfMappingsAfterLemmatization(int numberOfMappingsAfterLemmatization) {
        this.numberOfMappingsAfterLemmatization = numberOfMappingsAfterLemmatization;
    }
}

