package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.dao.AbstractDAO;
import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by cdursun on 7/7/2017.
 */
public class RnaSeqQuery  extends MappingSqlQuery {
    public RnaSeqQuery(DataSource ds, String query) {
        super(ds, query);
    }

    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {

        RnaSeq rnaSeq = new RnaSeq();

        rnaSeq.setKey(rs.getInt("KEY"));
        rnaSeq.setGeoAccessionId(rs.getString("GEO_ACCESSION_ID"));
        rnaSeq.setStudyTitle(rs.getString("STUDY_TITLE"));
        rnaSeq.setSubmissionDate(rs.getString("SUBMISSION_DATE"));
        rnaSeq.setPubmedId(rs.getString("PUBMED_ID"));
        rnaSeq.setPlatformId(rs.getString("PLATFORM_ID"));
        rnaSeq.setPlatformName(rs.getString("PLATFORM_NAME"));
        rnaSeq.setPlatformTechnology(rs.getString("PLATFORM_TECHNOLOGY"));
        rnaSeq.setTotalNumberOfSamples(rs.getString("TOTAL_NUMBER_OF_SAMPLES"));
        rnaSeq.setNumberOfRatSamples(rs.getString("NUMBER_OF_RAT_SAMPLES"));
        rnaSeq.setContributors(rs.getString("CONTRIBUTORS"));
        rnaSeq.setOverallDesign(rs.getString("OVERALL_DESIGN"));
        rnaSeq.setSampleAccessionId(rs.getString("SAMPLE_ACCESSION_ID"));
        rnaSeq.setSampleTitle(rs.getString("SAMPLE_TITLE"));
        rnaSeq.setSampleOrganism(rs.getString("SAMPLE_ORGANISM"));
        rnaSeq.setSampleSource(rs.getString("SAMPLE_SOURCE"));
        rnaSeq.setSampleAge(rs.getString("SAMPLE_AGE"));
        rnaSeq.setSampleGender(rs.getString("SAMPLE_GENDER"));
        rnaSeq.setSampleCellType(rs.getString("SAMPLE_CELL_TYPE"));
        rnaSeq.setSampleExtractionProtocol(rs.getString("SAMPLE_EXTRACT_PROTOCOL"));
        rnaSeq.setSampleTreatmentProtocol(rs.getString("SAMPLE_TREATMENT_PROTOCOL"));
        rnaSeq.setSummary(rs.getString("SUMMARY"));
        rnaSeq.setSampleDataProcessing(rs.getString("SAMPLE_DATA_PROCESSING"));
        rnaSeq.setSampleSupplementaryFiles(rs.getString("SAMPLE_SUPPLEMENTARY_FILES"));
        rnaSeq.setSampleCharacteristics(rs.getString("SAMPLE_CHARACTERISTICS"));
        rnaSeq.setSampleRelation(rs.getString("SAMPLE_RELATION"));
        rnaSeq.setSampleStrain(rs.getString("SAMPLE_STRAIN"));
        rnaSeq.setSampleCellLine(rs.getString("SAMPLE_CELL_LINE"));
        rnaSeq.setSampleGrowthProtocol(rs.getString("SAMPLE_GROWTH_PROTOCOL"));
        rnaSeq.setStudyRelation(rs.getString("STUDY_RELATION"));
        rnaSeq.setSampleTissue(rs.getString("SAMPLE_TISSUE"));
        rnaSeq.setRgdTissueTermAcc(rs.getString("RGD_TISSUE_TERM_ACC"));
        rnaSeq.setRgdCellTermAcc(rs.getString("RGD_CELL_TERM_ACC"));
        rnaSeq.setRgdStrainTermAcc(rs.getString("RGD_STRAIN_TERM_ACC"));
        rnaSeq.setRgdStrainRgdId(rs.getInt("RGD_STRAIN_RGD_ID"));

        return rnaSeq;
    }

    public static List<RnaSeq> execute(AbstractDAO dao, String sql, Object... params) throws Exception {
        RnaSeqQuery q = new RnaSeqQuery(dao.getDataSource(), sql);
        return dao.execute(q, params);
    }
}
