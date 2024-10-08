package edu.mcw.rgd.RNASeqPipeline;

/**
 * Created by cdursun on 5/22/2017.
 */

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.dao.spring.StringMapQuery;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DuplicateKeyException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

public class RnaSeqDAO extends AbstractDAO {
    private final Logger loggerSummary = LogManager.getLogger("summary");
    private final Logger loggerDuplicate = LogManager.getLogger("duplicate");

    private String crossSpeciesAnatomyOntId;
    private String ontTermExactSynonymType;
    private String cellOntId;
    private String ratStrainsOntId;


    public List<RnaSeq> getDataForGSE(String gseAccId) throws Exception {
        String sql = "SELECT * FROM rna_seq WHERE geo_accession_id=?";
        return RnaSeqQuery.execute(this, sql, gseAccId);
    }
    public List<String> getGeoIds(String gseAccId) throws Exception {
        String sql = "SELECT distinct(geo_accession_id) FROM rna_seq WHERE geo_accession_id like ?";
        return StringListQuery.execute(this, sql, gseAccId);
    }

    public Map<String,PubmedInfo> getPubmedIdsForAllGseAccessions( String species ) throws Exception {

        Map<String, PubmedInfo> results = new HashMap<>();

        String sql = "SELECT DISTINCT geo_accession_id,pubmed_id,curation_status FROM rna_seq WHERE sample_organism=?";
        try( Connection conn = getConnection() ) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, species);
            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {
                PubmedInfo info = new PubmedInfo();
                info.geoAcc = rs.getString(1);
                info.pubMedId = Utils.NVL(rs.getString(2), "");
                info.curationStatus = rs.getString(3);

                PubmedInfo info2 = results.get(info.geoAcc);
                if( info2!=null ) {
                    if( info2.pubMedId.equals(info.pubMedId) ) {
                        System.out.println("problem");
                    }
                }
                results.put(info.geoAcc, info);
            }
        }

        System.out.println("hash: "+results.size());
        return results;
    }

    // by default, CURATION_STATUS must be set to 'pending'
    public void insertRnaSeq(Series series){
        try {

            String platformTechnology = "";

            if (series.getPlatformList().size() != 0)
                platformTechnology = series.getPlatformList().get(0).getTechnology();

            boolean isDuplicateLogged = false;
            for( int i = 0; i < series.getSampleList().size(); i++) {
                Sample sample = series.getSampleList().get(i);
                String sql = """
                    INSERT INTO RNA_SEQ ( KEY, GEO_ACCESSION_ID, STUDY_TITLE, SUBMISSION_DATE, PUBMED_ID, SUMMARY, OVERALL_DESIGN, PLATFORM_ID,
                    PLATFORM_NAME, PLATFORM_TECHNOLOGY, TOTAL_NUMBER_OF_SAMPLES, NUMBER_OF_RAT_SAMPLES, STUDY_RELATION, CONTRIBUTORS, SAMPLE_ACCESSION_ID,
                    SAMPLE_TITLE, SAMPLE_ORGANISM, SAMPLE_SOURCE, SAMPLE_CHARACTERISTICS, SAMPLE_STRAIN, SAMPLE_AGE, SAMPLE_GENDER, SAMPLE_TISSUE,
                    SAMPLE_CELL_TYPE, SAMPLE_CELL_LINE, SAMPLE_GROWTH_PROTOCOL, SAMPLE_EXTRACT_PROTOCOL, SAMPLE_TREATMENT_PROTOCOL, SAMPLE_DATA_PROCESSING,
                    SAMPLE_SUPPLEMENTARY_FILES, SAMPLE_RELATION, SUPPLEMENTARY_FILES, CURATION_STATUS )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,'pending')
                    """;

                // skip samples for species that are not in RGD
                String incomingOrganism = sample.getOrganism_ch1();
                int speciesTypeKey = SpeciesType.parse(incomingOrganism);
                if( speciesTypeKey <= 0 ) {
                    continue;
                }

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

                try {
                    this.update(sql, array);
                } catch (DuplicateKeyException dke) {
                    // because of download indexes sometimes the same file could be inserted
                    // just log per file (not for all sample records in the series file) and ignore it
                    if (!isDuplicateLogged) { // in order to log per file
                        loggerDuplicate.info(series.getGeoAccessionID()+" "+sample.getGeoAccessionID());
                        isDuplicateLogged = true;
                    }
                }
            }
        }
        catch(Exception e){
            loggerSummary.error("RnaSeqDAO.insertRnaSeq() : " + e.getMessage());
        }
    }




    /**
     * get list of all RnaSeq with some of the fields required to map RGD DB
     * @return list of RnaSeq objects
     * @throws Exception when something really bad happens in spring framework
     */
    public List<RnaSeq> getAllRnaSeq(Date dateCutoff) throws Exception{

        List<String> organisms = new ArrayList<>();
        for( int sp: SpeciesType.getSpeciesTypeKeys() ) {
            if( sp>0 ) {
                organisms.add( SpeciesType.getTaxonomicName(sp).toLowerCase() );
            }
        }
        String organismList = Utils.concatenate(organisms, ",", "'");

        ArrayList rnaSeqList = new ArrayList();

        try (Connection conn = this.getConnection() ){
            String sql = "select KEY,SAMPLE_TISSUE, SAMPLE_STRAIN, SAMPLE_CELL_LINE, SAMPLE_CELL_TYPE, RGD_TISSUE_TERM_ACC, RGD_CELL_TERM_ACC, RGD_STRAIN_TERM_ACC, RGD_STRAIN_RGD_ID " +
                    "FROM rna_seq where LOWER(sample_organism) IN ("+organismList+") " +
                    "AND geo_accession_id not in ('GSE50027','GSE53960') " +
                    "AND created_in_rgd>? AND date_mapped IS NULL";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setTimestamp(1, new Timestamp(dateCutoff.getTime()));
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
            }

            loggerSummary.info("Total number of RnaSeq records pulled from DB : " + rnaSeqList.size());
            return rnaSeqList;
        }
    }

    public void updateRgdMappingFields(List<RnaSeq> rnaSeqList) throws Exception {
        Date time0 = Calendar.getInstance().getTime();
        for (RnaSeq r : rnaSeqList) {
            updateRgdFields(r);
        }
        loggerSummary.info("==========> Update Ont Terms Time : " + Utils.formatElapsedTime(time0.getTime(), System.currentTimeMillis()) + ". -----");
    }

    public int checkConnection() throws Exception{
        String q = "SELECT 1 FROM DUAL";
        return this.getCount(q);
    }

    /**
     * Update RnaSeq rgdOntTermAcc fields and rgdStrainRgdId field based on key
     *
     * @param rnaSeq
     * @throws Exception
     */
    public int updateRgdFields(RnaSeq rnaSeq) throws Exception{

        String sql = "UPDATE rna_seq SET RGD_TISSUE_TERM_ACC=?, RGD_CELL_TERM_ACC=?, RGD_STRAIN_TERM_ACC=?, RGD_STRAIN_RGD_ID=?, DATE_MAPPED=SYSDATE "+
                "WHERE key=? ";

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

    public class PubmedInfo {
        public String geoAcc;
        public String pubMedId;
        public String curationStatus;
    }
}
