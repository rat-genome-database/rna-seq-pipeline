package edu.mcw.rgd.RNASeqPipeline;

/**
 * Created by cdursun on 5/22/2017.
 */

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DuplicateKeyException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class RnaSeqDAO extends AbstractDAO {
    private final Log loggerColumnSize = LogFactory.getLog("column_size");
    private final Log loggerSummary = LogFactory.getLog("dublicate");
    private final Log loggerDublicate = LogFactory.getLog("summary");

    private String crossSpeciesAnatomyOntId;
    private String ontTermExactSynonymType;
    private String cellOntId;
    private String ratStrainsOntId;

    public List<RnaSeq> getDataForGSE(String gseAccId) throws Exception {
        String sql = "SELECT * FROM rna_seq WHERE geo_accession_id=?";
        return RnaSeqQuery.execute(this, sql, gseAccId);
    }

    public void insertRnaSeq(Series series){
        try {
            String platformTechnology = "";

            if (series.getPlatformList().size() != 0)
                platformTechnology = series.getPlatformList().get(0).getTechnology();

            boolean isDublicateLogged = false;
            for (int i = 0; i < series.getSampleList().size(); i++) {
                Sample sample = series.getSampleList().get(i);
                String sql = "INSERT INTO DEV_1.RNA_SEQ ( KEY, GEO_ACCESSION_ID, STUDY_TITLE, SUBMISSION_DATE, PUBMED_ID, SUMMARY, OVERALL_DESIGN, PLATFORM_ID, " +
                        "PLATFORM_NAME, PLATFORM_TECHNOLOGY, TOTAL_NUMBER_OF_SAMPLES, NUMBER_OF_RAT_SAMPLES, STUDY_RELATION, CONTRIBUTORS, SAMPLE_ACCESSION_ID, " +
                        "SAMPLE_TITLE, SAMPLE_ORGANISM, SAMPLE_SOURCE, SAMPLE_CHARACTERISTICS, SAMPLE_STRAIN, SAMPLE_AGE, SAMPLE_GENDER, SAMPLE_TISSUE, " +
                        "SAMPLE_CELL_TYPE, SAMPLE_CELL_LINE, SAMPLE_GROWTH_PROTOCOL, SAMPLE_EXTRACT_PROTOCOL, SAMPLE_TREATMENT_PROTOCOL, SAMPLE_DATA_PROCESSING, " +
                        "SAMPLE_SUPPLEMENTARY_FILES, SAMPLE_RELATION, SUPPLEMENTARY_FILES ) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?)";

                int key = this.getNextKey("GEO_SEQ");

                Object[] array = new Object[]{
                        Integer.valueOf(key),                                       /*0*/
                        series.getGeoAccessionID(),                                      /*1*/
                        series.getTitle(),                                               /*2*/
                        series.getSubmissionDate(),                                      /*3*/
                        series.getPubmedID(),                                            /*4*/
                        series.getSummary(),                                             /*5*/
                        series.getOverallDesign().getStoreStr().replace("||||", "||"),   /*6*/
                        series.getPlatformID(),                                          /*7*/
                        series.findPlatformName(sample.getPlatformID()),                 /*8*/
                        platformTechnology,                                         /*9*/
                        series.getSampleID().getStoreLength(),                           /*10*/
                        series.getNumRatSamples(),                                  /*11*/
                        series.getRelation().getStoreStr(),                              /*12*/
                        series.getContributor().getStoreStr(),                           /*13*/
                        sample.getGeoAccessionID(),                                      /*14*/
                        sample.getTitle(),                                               /*15*/
                        sample.getOrganism_ch1(),                                        /*16*/
                        sample.getSourceName_ch1(),                                      /*17*/
                        sample.getCharacteristics_ch1().getStoreStr(),                   /*18*/
                        sample.getStrain(),                                              /*19*/
                        sample.getAge(),                                                 /*20*/
                        sample.getGender(),                                             /*21*/
                        sample.getTissue(),                                             /*22*/
                        sample.getCellType(),                                           /*23*/
                        sample.getCellLine(),                                           /*24*/
                        sample.getGrowthProtocol_ch1().getStoreStr(),                    /*25*/
                        sample.getExtractProtocol_ch1().getStoreStr(),                   /*26*/
                        sample.getTreatmentProtocol_ch1(),                               /*27*/
                        sample.getDataProcessing().getStoreStr(),                        /*28*/
                        sample.getSupplementaryFile().getStoreStr(),                     /*29*/
                        sample.getRelation().getStoreStr(),                              /*30*/
                        series.getSupplementaryFile().getStoreStr()                      /*31*/
                };
                loggerColumnSize.info("----------------------------- " + series.getGeoAccessionID() + " --------------------------------------------------");
                for (int j = 0; j < array.length; j++) {
                    loggerColumnSize.info(j + " : " + (array[j] == null ? null : array[j].getClass() == String.class ? ((String) array[j]).length() : "int"));
                }
                loggerColumnSize.info("-------------------------------------------------------------------------------");
                try {
                    this.update(sql, array);
                } catch (DuplicateKeyException dke) {
                    // because of download indexes sometimes the same file could be inserted
                    // just log per file (not for all sample records in the series file) and ignore it
                    if (!isDublicateLogged) { // in order to log per file
                        loggerDublicate.info(series.getGeoAccessionID());
                        isDublicateLogged = true;
                    }
                }
            }
        }
        catch(Exception e){
            loggerSummary.error("RnaSeqDAO.insertRnaSeq() : " + e);
        }
    }

    /**
     * get list of all RnaSeq with some of the fields required to map RGD DB
     * @return list of RnaSeq objects
     * @throws Exception when something really bad happens in spring framework
     */
    /*public List<RnaSeq> mapRnaSeqToRgd() throws Exception{

        ArrayList rnaSeqList = new ArrayList();
        int numberOfMappedRecords = 0;

        List<Term> crossSpeciesTerms = ontologyXDAO.getActiveTerms(crossSpeciesAnatomyOntId);
        List<TermSynonym> crossSpeciesSynoyms = ontologyXDAO.getActiveSynonyms(crossSpeciesAnatomyOntId);
        List<Term> cellOntTerms = ontologyXDAO.getActiveTerms(cellOntId);
        List<TermSynonym> cellOntSynoyms = ontologyXDAO.getActiveSynonyms(cellOntId);
        List<Term> ratStrainOntTerms = ontologyXDAO.getActiveTerms(ratStrainsOntId);
        List<TermSynonym> ratStrainOntSynoyms = ontologyXDAO.getActiveSynonyms(ratStrainsOntId);
        List<Strain> strains = strainDAO.getActiveStrains();


        loggerSummary.info("UBERON Terms size : " + crossSpeciesTerms.size());
        loggerSummary.info("UBERON Synonyms size : " + crossSpeciesSynoyms.size());
        loggerSummary.info("CL Terms size : " + cellOntTerms.size());
        loggerSummary.info("CL Synonyms size : " + cellOntSynoyms.size());
        loggerSummary.info("RS Terms size : " + ratStrainOntTerms.size());
        loggerSummary.info("RS Synonyms size : " + ratStrainOntSynoyms.size());

        int i = 1;
        Connection conn = null;


        try {
            conn =  this.getConnection();
            String sql = "select KEY,SAMPLE_TISSUE, RGD_TISSUE_TERM_ACC, RGD_CELL_TERM_ACC, RGD_CELL_TYPE_TERM_ACC, RGD_TISSUE_TERM_ACC, RGD_STRAIN_RGD_ID " +
                    "from rna_seq where LOWER(sample_organism)='rattus norvegicus' or LOWER(sample_organism)='homo sapiens' " +
                    "or LOWER(sample_organism)='mus musculus' \n" +
                    "or LOWER(sample_organism)='chinchilla lanigera' or LOWER(sample_organism)='pan paniscus' or LOWER(sample_organism)='canis lupus familiaris'\n" +
                    "or LOWER(sample_organism)='ictidomys tridecemlineatus' or LOWER(sample_organism)='danio rerio' ";

            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                RnaSeq rnaSeq = new RnaSeq();
                rnaSeq.setKey(rs.getInt("KEY"));
                rnaSeq.setSampleTissue(rs.getString("SAMPLE_TISSUE"));
                rnaSeq.setRgdTissueTermAcc(rs.getString("RGD_TISSUE_TERM_ACC"));
                rnaSeq.setRgdCellTermAcc(rs.getString("RGD_CELL_TERM_ACC"));
                rnaSeq.setRgdCellTypeTermAcc(rs.getString("RGD_CELL_TYPE_TERM_ACC"));
                rnaSeq.setRgdStrainTermAcc(rs.getString("RGD_TISSUE_TERM_ACC"));
                rnaSeq.setRgdStrainRgdId(rs.getInt("RGD_STRAIN_RGD_ID"));

                byte result = mapByOntTerm(rnaSeq.getSampleTissue(), rnaSeq, crossSpeciesTerms);

                if (result == 0)
                    result = mapByOntTerm(rnaSeq.getSampleCellLine(), rnaSeq, cellOntTerms);
                else if (result==0)
                    result = mapByOntTerm(rnaSeq.getSampleStrain(), rnaSeq, ratStrainOntTerms);
                else if (result == 0)
                    result = mapByOntSynoym(rnaSeq.getSampleTissue(), rnaSeq, crossSpeciesSynoyms);
                else if (result == 0)
                    result = mapByOntSynoym(rnaSeq.getSampleCellLine(), rnaSeq, cellOntSynoyms);
                else if (result == 0)
                    result = mapByOntSynoym(rnaSeq.getSampleStrain(), rnaSeq, ratStrainOntSynoyms);

                if (result == 1)
                    numberOfMappedRecords++;

                if (i % 10000 == 0)
                    loggerSummary.info("processed: " + i++);
            }

            loggerSummary.info("Total number of records by Term & Sample Tissue : " + numberOfMappedRecords);

            return rnaSeqList;
        } finally {
            try {
                conn.close();
            }catch (Exception ignored) {
            }
        }
    }*/



    /**
     * get list of all RnaSeq with some of the fields required to map RGD DB
     * @return list of RnaSeq objects
     * @throws Exception when something really bad happens in spring framework
     */
    public List<RnaSeq> getAllRnaSeq() throws Exception{

        ArrayList rnaSeqList = new ArrayList();
        int numberOfMappedRecords = 0;

        int i = 1;
        Connection conn = null;

        try {
            conn =  this.getConnection();
            String sql = "select KEY,SAMPLE_TISSUE, SAMPLE_STRAIN, SAMPLE_CELL_LINE, SAMPLE_CELL_TYPE, RGD_TISSUE_TERM_ACC, RGD_CELL_TERM_ACC, RGD_STRAIN_TERM_ACC, RGD_STRAIN_RGD_ID " +
                    "from rna_seq where (LOWER(sample_organism)='rattus norvegicus' or LOWER(sample_organism)='homo sapiens' " +
                    "or LOWER(sample_organism)='mus musculus' \n" +
                    "or LOWER(sample_organism)='chinchilla lanigera' or LOWER(sample_organism)='pan paniscus' or LOWER(sample_organism)='canis lupus familiaris'\n" +
                    "or LOWER(sample_organism)='ictidomys tridecemlineatus' or LOWER(sample_organism)='danio rerio')"; //

            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();


            while (rs.next()/* && i <= 4000*/) {
                RnaSeq rnaSeq = new RnaSeq();
                rnaSeq.setKey(rs.getInt("KEY"));
                rnaSeq.setSampleTissue(rs.getString("SAMPLE_TISSUE"));
                rnaSeq.setSampleStrain(rs.getString("SAMPLE_STRAIN"));
                rnaSeq.setSampleCellLine(rs.getString("SAMPLE_CELL_LINE"));
                rnaSeq.setSampleCellType(rs.getString("SAMPLE_CELL_TYPE"));
                //RGD_ fields should be null, at each run these fields will be reset
                /*rnaSeq.setRgdTissueTermAcc(rs.getString("RGD_TISSUE_TERM_ACC"));
                rnaSeq.setRgdCellTermAcc(rs.getString("RGD_CELL_TERM_ACC"));
                rnaSeq.setRgdStrainTermAcc(rs.getString("RGD_STRAIN_TERM_ACC"));
                rnaSeq.setRgdStrainRgdId(rs.getInt("RGD_STRAIN_RGD_ID"));*/

                rnaSeqList.add(rnaSeq);
                i++;
            }

            loggerSummary.info("Total number of RnaSeq records pulled from DB : " + rnaSeqList.size());
            return rnaSeqList;
        } finally {
            try {
                conn.close();
            }catch (Exception ignored) {
            }
        }
    }

    public void updateRgdMappingFields(List<RnaSeq> rnaSeqList) throws Exception {
        Date time0 = Calendar.getInstance().getTime();
        for (RnaSeq r : rnaSeqList) {
            updateRgdFields(r);
        }
        loggerSummary.info("==========> Update Ont Terms Time : " + Utils.formatElapsedTime(time0.getTime(), System.currentTimeMillis()) + ". -----");
    }

    public void updateRgdStrainRgdIds(Set<RnaSeq> rnaSeqList) throws Exception {
        for (RnaSeq r : rnaSeqList) {
            updateRgdStrainRgdId(r);
        }
    }

    public int checkConnection() throws Exception{
        String q = "SELECT 1 FROM DUAL";
        return this.getCount(q);
    }
  /*  public byte mapByOntTerm(String rnaSeqString, RnaSeq rnaSeq, List<Term>  terms ) throws Exception{
        if(rnaSeqString == null) return 0;
        for (Term term : terms) {
            if (rnaSeqString.toLowerCase().equals(term.getTerm().toLowerCase())) {
                rnaSeq.setRgdOntTermAcc(term.getAccId());
                updateRgdOntTermAcc(rnaSeq);
                return 1;
            }
        }
        return 0;
    }

    public byte mapByOntSynoym(String rnaSeqString, RnaSeq rnaSeq, List<TermSynonym>  synonyms ) throws Exception{
        if(rnaSeqString == null) return 0;
        for (TermSynonym synonym : synonyms) {
            if (synonym.getType().equals(ontTermExactSynonymType) && rnaSeqString.toLowerCase().equals(synonym.getName().toLowerCase())){
                rnaSeq.setRgdOntTermAcc(synonym.getTermAcc());
                updateRgdOntTermAcc(rnaSeq);
                return 1;
            }
        }
        return 0;
    }*/

    /**
     * Update RnaSeq rgdOntTermAcc fields and rgdStrainRgdId field based on key
     *
     * @param rnaSeq
     * @throws Exception
     */
    public int updateRgdFields(RnaSeq rnaSeq) throws Exception{

        String sql = "UPDATE rna_seq SET RGD_TISSUE_TERM_ACC=?, RGD_CELL_TERM_ACC = ?, RGD_STRAIN_TERM_ACC=?, RGD_STRAIN_RGD_ID = ? WHERE key=? ";

        return update(sql, rnaSeq.getRgdTissueTermAcc(), rnaSeq.getRgdCellTermAcc(),
                rnaSeq.getRgdStrainTermAcc(), rnaSeq.getRgdStrainRgdId(), rnaSeq.getKey());
    }

    /**
     * Update RnaSeq rgdStrainRgdId field based on key
     *
     * @param rnaSeq
     * @throws Exception
     */
    public int updateRgdStrainRgdId(RnaSeq rnaSeq) throws Exception{

        String sql = "UPDATE rna_seq SET RGD_STRAIN_RGD_ID = ? WHERE key=? ";
        return update(sql, rnaSeq.getRgdStrainRgdId(), rnaSeq.getKey());
    }

    /**
     * Update rnaSeq in the database based on key
     *
     * @param rnaSeq
     * @throws Exception
     */
    public void updateRnaSeq(RnaSeq rnaSeq) throws Exception{

        String sql = "update RnaSeq set GEO_ACCESSION_ID=?, STUDY_TITLE=?, SUBMISSION_DATE=?, " +
                "PUBMED_ID=?, PLATFORM_ID=?, PLATFORM_NAME=?, PLATFORM_TECHNOLOGY=?, TOTAL_NUMBER_OF_SAMPLES=?, NUMBER_OF_RAT_SAMPLES=?, CONTRIBUTORS=?,  SAMPLE_ACCESSION_ID=?, " +
                "SAMPLE_TITLE=?, SAMPLE_ORGANISM=?, SAMPLE_SOURCE=?, SAMPLE_AGE=?, SAMPLE_GENDER=?, SAMPLE_CELL_TYPE=?, OVERALL_DESIGN=?, " +
                "SAMPLE_EXTRACT_PROTOCOL=?, SAMPLE_TREATMENT_PROTOCOL=?, SUMMARY=?, SAMPLE_DATA_PROCESSING=?, SAMPLE_SUPPLEMENTARY_FILES=?, SAMPLE_CHARACTERISTICS=?, " +
                "SAMPLE_RELATION=?, SAMPLE_STRAIN=?, SAMPLE_CELL_LINE=? , SAMPLE_GROWTH_PROTOCOL=?, STUDY_RELATION=?, SAMPLE_TISSUE=?,  RGD_TISSUE_TERM_ACC=?, " +
                "RGD_CELL_TERM_ACC=?, RGD_STRAIN_TERM_ACC=?, RGD_STRAIN_RGD_ID=?  where KEY=?";

        Object[] oa = new Object[]{rnaSeq.getGeoAccessionId(), rnaSeq.getStudyTitle(), rnaSeq.getSubmissionDate(), rnaSeq.getPubmedId(), rnaSeq.getPlatformId(),
                rnaSeq.getPlatformId(), rnaSeq.getPlatformName(), rnaSeq.getTotalNumberOfSamples(), rnaSeq.getNumberOfRatSamples(), rnaSeq.getContributors(),
                rnaSeq.getSampleAccessionId(), rnaSeq.getSampleTitle(), rnaSeq.getSampleOrganism(), rnaSeq.getSampleSource(), rnaSeq.getSampleAge(),
                rnaSeq.getSampleGender(), rnaSeq.getSampleCellType(), rnaSeq.getOverallDesign(), rnaSeq.getSampleExtractionProtocol(),
                rnaSeq.getSampleTreatmentProtocol(), rnaSeq.getSummary(), rnaSeq.getSampleDataProcessing(), rnaSeq.getSampleSupplementaryFiles(),
                rnaSeq.getSampleCharacteristics(), rnaSeq.getSampleRelation(), rnaSeq.getSampleStrain(), rnaSeq.getSampleCellLine(), rnaSeq.getSampleGrowthProtocol(),
                rnaSeq.getStudyRelation(), rnaSeq.getSampleTissue(), rnaSeq.getRgdTissueTermAcc(), rnaSeq.getRgdCellTermAcc(),
                rnaSeq.getRgdStrainTermAcc(), rnaSeq.getRgdStrainRgdId(), rnaSeq.getKey()};

        update(sql, oa);

    }

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

    public class RnaSeqDAOException extends Exception {
        public RnaSeqDAOException(String msg) {
            super(msg);
        }
    }

}
