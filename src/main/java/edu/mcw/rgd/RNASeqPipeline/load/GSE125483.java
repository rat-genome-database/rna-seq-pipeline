package edu.mcw.rgd.RNASeqPipeline.load;

import edu.mcw.rgd.RNASeqPipeline.RnaSeq;
import edu.mcw.rgd.RNASeqPipeline.RnaSeqDAO;
import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.GeneExpressionDAO;
import edu.mcw.rgd.dao.impl.PhenominerDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.datamodel.pheno.*;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created by hsnalabolu on 3/4/2020.
 */
public class GSE125483 {
    GeneExpressionDAO gedao = new GeneExpressionDAO();
    XdbIdDAO xdao = new XdbIdDAO();
    RnaSeqDAO dao = new RnaSeqDAO();
    PhenominerDAO pdao = new PhenominerDAO();

    private Map<String,Integer> geneExpressionRecordMap = new HashMap<>();
    private Map<Integer,String> cmoIDs = new HashMap<>();


    public static void main(String[] args) throws Exception {

        try {
            new GSE125483().run();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    void run() throws Exception {

        String gseAcc = "GSE125483";
        final int STUDY_ID = 3068;
        final boolean firstRun = true;
        int mapKey = 360;
        String file = "/data/rat/GSE125483_rat.salmon.tximport.tpm.txt";

        // load all RnaSeq data from DB
        List<RnaSeq> rnaSeqs = dao.getDataForGSE(gseAcc);

        // process all samples
            for( RnaSeq rs: rnaSeqs ) {
                if (rs.getSampleOrganism().startsWith("Rattus")) {
                    int experimentId = 0;
                    int sampleId = 0;
                    Sample sample = new Sample();
                    Experiment experiment = new Experiment();

                    String part = rs.getSampleTissue();
                    String header="Rat_";
                    String sampleTitle = rs.getSampleTitle();

                    sampleTitle = sampleTitle.substring(0,8);

                    sample = parseAge(sample, rs.getSampleAge());
                    sample.setNumberOfAnimals(1);

                    if (rs.getSampleGender().equals("F")) {
                        sample.setSex("female");
                    } else if (rs.getSampleGender().equals("M")) {
                        sample.setSex("male");
                    } else {
                        throw new Exception("unknown sample sex");
                    }

                    sample.setStrainAccId(rs.getRgdStrainTermAcc());
                    sample.setTissueAccId(rs.getRgdTissueTermAcc());
                    sample.setCellTypeAccId(rs.getRgdCellTermAcc());
                    sample.setGeoSampleAcc(rs.getSampleAccessionId());
                    sample.setNotes(rs.getSampleTitle());
                    sample.setBioSampleId(parseBioSampleId(rs.getSampleRelation()));
                    Sample s;

                    if (firstRun == false)
                        s = dao.getSampleFromBioSampleId(sample);
                    else
                        s = dao.getSample(sample);
                    if (s == null) {
                        sampleId = pdao.insertSample(sample);
                        System.out.println("Inserted Sample :" + sampleId);
                    } else {
                        sampleId = s.getId();
                        if (s.getBioSampleId() == null || !s.getBioSampleId().contains(sample.getBioSampleId())) {
                            dao.updateBioSampleId(sampleId, sample);
                            System.out.println("Updated Sample :" + sampleId);
                        }
                    }
                    sample.setId(sampleId);
                    experiment.setStudyId(STUDY_ID);
                    experiment.setName(getExperimentName(part.toLowerCase()));
                    experiment.setTraitOntId(getTraitId(part, getExperimentName(part.toLowerCase())));
                    experimentId = dao.getExperimentId(experiment);
                    if (experimentId == 0) {
                        experimentId = pdao.insertExperiment(experiment);
                        System.out.println("Inserted experiment :" + experimentId);
                    }


                    experiment.setId(experimentId);


                    GeneExpressionRecord ger = new GeneExpressionRecord();
                    ger.setExperimentId(experimentId);
                    ger.setSampleId(sample.getId());
                    ger.setLastModifiedBy("hsnalabolu");
                    ger.setLastModifiedDate(new Date());
                    ger.setCurationStatus(20);
                    ger.setSpeciesTypeKey(SpeciesType.RAT);
                    int geneExprRecId = dao.getGeneExprRecordId(ger);
                    if (geneExprRecId == 0) {
                        geneExprRecId = gedao.insertGeneExpressionRecord(ger);
                        // every record must have a one condition
                        //addConditions(ger);

                        // every record must have a one measurement method
                        //addMeasurementMethods(ger);
                        System.out.println("Inserted geneExpressionRecord :" + geneExprRecId);
                    }
                    ger.setId(geneExprRecId);



                    String cmoId = getCMOId(part);
                    cmoIDs.put(geneExprRecId, cmoId);
                    String sex;
                    if(sample.getSex().equals("male"))
                        sex = "Male";
                    else sex = "Female";
                    header = header.concat(sex).concat("_").concat(part).concat("_").concat(sampleTitle);
                    //System.out.println(header);
                    geneExpressionRecordMap.put(header, geneExprRecId);

                }
            }
        System.out.println(geneExpressionRecordMap);
        loadValues(mapKey,file);
    }

    Sample parseAge(Sample sample, String sampleAge) throws Exception {
        if( sampleAge.endsWith(" weeks") ) {
            String weeks = sampleAge.substring(0, sampleAge.length()-" weeks".length()).trim();
            int ageInDays = 7 * Integer.parseInt(weeks);
            sample.setAgeDaysFromHighBound(ageInDays);
            sample.setAgeDaysFromLowBound(ageInDays);
        }else if( sampleAge.endsWith(" months") ) {
            String months = sampleAge.substring(0, sampleAge.length()-" months".length()).trim();
            int ageInDays = 30 * Integer.parseInt(months);
            sample.setAgeDaysFromHighBound(ageInDays);
            sample.setAgeDaysFromLowBound(ageInDays);
        }else if( sampleAge.endsWith(" days") ) {
            String days = sampleAge.substring(0, sampleAge.length()-" days".length()).trim();
            int ageInDays = Integer.parseInt(days);
            sample.setAgeDaysFromHighBound(ageInDays);
            sample.setAgeDaysFromLowBound(ageInDays);
        }
        else {
            throw new Exception("unknown sample age: " + sampleAge);
        }
        return sample;
    }
    public String getCMOId(String part) throws Exception{

        String cmoId = "";
        String term = part + " ribonucleic acid composition measurement";
        if(part.equalsIgnoreCase("diencephalon and midbrain") || part.equalsIgnoreCase("forebrain and midbrain") || part.equalsIgnoreCase("hindbrain without cerebellum")
                || part.equalsIgnoreCase("pituitary and diencephalon"))
            term = "combined " + term;

        if(part.equalsIgnoreCase("forebrain") || part.equalsIgnoreCase("hindbrain") || part.equalsIgnoreCase("brain fragment") || part.equalsIgnoreCase("midbrain"))
            cmoId = dao.getTermByTermName("brain ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("frontal lobe") || part.equalsIgnoreCase("temporal lobe") || part.equalsIgnoreCase("prefrontal cortex") || part.equalsIgnoreCase("cerebellum"))
            cmoId = dao.getTermByTermName("cerebrum ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("skeletal muscle tissue"))
            cmoId = dao.getTermByTermName("skeletal muscle ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("smooth muscle tissue"))
            cmoId = dao.getTermByTermName("smooth muscle ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("breast"))
            cmoId = dao.getTermByTermName("mammary organ ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("leukocyte"))
            cmoId = dao.getTermByTermName("white blood cell ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("sigmoid colon"))
            cmoId = dao.getTermByTermName("colon ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("duodenum"))
            cmoId = dao.getTermByTermName("small intestine ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("endometrium"))
            cmoId = dao.getTermByTermName("uterus ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("fallopian tube"))
            cmoId = dao.getTermByTermName("oviduct ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("saliva-secreting gland"))
            cmoId = dao.getTermByTermName("salivary gland ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("vermiform appendix"))
            cmoId = dao.getTermByTermName("appendix ribonucleic acid composition measurement","CMO");
        else if (part.equalsIgnoreCase("zone of skin"))
            cmoId = dao.getTermByTermName("skin ribonucleic acid composition measurement","CMO");
        else if(part.equalsIgnoreCase("forebrain fragment"))
            cmoId = dao.getTermByTermName("forebrain ribonucleic acid composition measurement","CMO");
        else if(part.equalsIgnoreCase("hindbrain fragment"))
            cmoId = dao.getTermByTermName("hindbrain ribonucleic acid composition measurement","CMO");
        else if(part.equalsIgnoreCase("hippocampus"))
            cmoId = dao.getTermByTermName("hippocampus proper ribonucleic acid composition measurement","CMO");
        else
            cmoId = dao.getTermByTermName(term,"CMO");

        return cmoId;
    }
    public String getExperimentName(String part) throws Exception{

        System.out.println(part);
        String exprName = part + " molecular composition trait";
        if(part.equalsIgnoreCase("forebrain") || part.equalsIgnoreCase("hindbrain") || part.equalsIgnoreCase("amygdala") || part.equalsIgnoreCase("brain meninx")
                || part.equalsIgnoreCase("Brodmann (1909) area 24") || part.equalsIgnoreCase("Brodmann (1909) area 9") || part.equalsIgnoreCase("caudate nucleus") || part.equalsIgnoreCase("cerebellar hemisphere")
                || part.equalsIgnoreCase("cerebral cortex") || part.equalsIgnoreCase("diencephalon") || part.equalsIgnoreCase("dorsal thalamus") || part.equalsIgnoreCase("globus pallidus")
                || part.equalsIgnoreCase("hippocampal formation") || part.equalsIgnoreCase("hippocampus proper") || part.equalsIgnoreCase("hypothalamus") || part.equalsIgnoreCase("locus ceruleus")
                || part.equalsIgnoreCase("medulla oblongata") || part.equalsIgnoreCase("middle frontal gyrus") || part.equalsIgnoreCase("middle temporal gyrus") || part.equalsIgnoreCase("nucleus accumbens")
                || part.equalsIgnoreCase("occipital cortex") || part.equalsIgnoreCase("occipital lobe") || part.equalsIgnoreCase("parietal lobe") || part.equalsIgnoreCase("pineal body")
                 || part.equalsIgnoreCase("putamen") || part.equalsIgnoreCase("substantia nigra") || part.equalsIgnoreCase("Brodmann area")
                || part.equalsIgnoreCase("basal ganglion")  || part.equalsIgnoreCase("choroid plexus")  || part.equalsIgnoreCase("telencephalon") || part.equalsIgnoreCase("pons")
                || part.equalsIgnoreCase("midbrain") || part.equalsIgnoreCase("forebrain fragment") || part.equalsIgnoreCase("diencephalon and midbrain") || part.equalsIgnoreCase("forebrain and midbrain")
                || part.equalsIgnoreCase("hindbrain without cerebellum") || part.equalsIgnoreCase("pituitary and diencephalon") || part.equalsIgnoreCase("brain fragment") || part.equalsIgnoreCase("hindbrain fragment")
                || part.equalsIgnoreCase("hippocampus") || part.equalsIgnoreCase("corpus callosum") || part.equalsIgnoreCase("brainstem") || part.equalsIgnoreCase("frontal cortex")
                || part.equalsIgnoreCase("dorsal plus ventral thalamus"))

            exprName = "brain molecular composition trait";
        else if(part.equalsIgnoreCase("skeletal muscle tissue") || part.equalsIgnoreCase("smooth muscle tissue") || part.equalsIgnoreCase("diaphragm") || part.equalsIgnoreCase("muscle of arm")
                || part.equalsIgnoreCase("muscle of leg") || part.equalsIgnoreCase("skeletal muscle of trunk") || part.equalsIgnoreCase("skeletal muscle organ") || part.equalsIgnoreCase("musculature")){
            exprName = "muscle molecular composition trait";
        }else if(part.equalsIgnoreCase("adipose tissue") || part.equalsIgnoreCase("subcutaneous adipose tissue")){
            exprName = "adipose molecular composition trait";
        }else if(part.equalsIgnoreCase("breast")) {
            exprName = "mammary gland morphology trait";
        }else if(part.equalsIgnoreCase("prostate gland")) {
            exprName = "prostate morphology trait";
        }else if(part.equalsIgnoreCase("sigmoid colon")) {
            exprName = "colon morphology trait";
        } else if (part.equalsIgnoreCase("frontal lobe") || part.equalsIgnoreCase("temporal lobe") || part.equalsIgnoreCase("prefrontal cortex") || part.equalsIgnoreCase("cerebellum")){
            exprName = "cerebrum morphology trait";
        } else if(part.equalsIgnoreCase("saliva-secreting gland")){
            exprName = "salivary gland morphology trait";
        }else if(part.equalsIgnoreCase("bone marrow")){
            exprName = "bone marrow cell morphology trait";
        }else if(part.equalsIgnoreCase("duodenum")){
            exprName = "small intestine morphology trait";
        }else if(part.equalsIgnoreCase("endometrium")){
            exprName = "uterus molecular composition trait";
        }else if(part.equalsIgnoreCase("fallopian tube")){
            exprName = "oviduct morphology trait";
        }else if(part.equalsIgnoreCase("ectocervix") || part.equalsIgnoreCase("endocervix") || part.equalsIgnoreCase("uterine cervix") || part.equalsIgnoreCase("vagina")
                || part.equalsIgnoreCase("vulva") || part.equalsIgnoreCase("mammary")){
            exprName = "female reproductive system morphology trait";
        }else if(part.equalsIgnoreCase("epididymis") || part.equalsIgnoreCase("penis") || part.equalsIgnoreCase("seminal vesicle") || part.equalsIgnoreCase("vas deferens")){
            exprName = "male reproductive system morphology trait";
        }else if(part.equalsIgnoreCase("vermiform appendix") || part.equalsIgnoreCase("esophagogastric junction") || part.equalsIgnoreCase("large intestine")
                || part.equalsIgnoreCase("transverse colon") || part.equalsIgnoreCase("esophagus mucosa") || part.equalsIgnoreCase("esophagus muscularis mucosa")
                || part.equalsIgnoreCase("greater omentum") || part.equalsIgnoreCase("minor salivary gland") || part.equalsIgnoreCase("mouth mucosa")
                || part.equalsIgnoreCase("parotid gland") || part.equalsIgnoreCase("submandibular gland") ){
            exprName = "gastrointestinal system morphology trait";
        }else if(part.equalsIgnoreCase("zone of skin") || part.equalsIgnoreCase("transformed skin fibroblast") || part.equalsIgnoreCase("lower leg skin") || part.equalsIgnoreCase("suprapubic skin")
                || part.equalsIgnoreCase("hair follicle") || part.equalsIgnoreCase("fibroblast")){
            exprName = "skin molecular composition trait";
        }else if(part.equalsIgnoreCase("aorta") || part.equalsIgnoreCase("coronary artery") || part.equalsIgnoreCase("tibial artery")){
            exprName = "artery molecular composition trait";
        }else if(part.equalsIgnoreCase("cortex of kidney") || part.equalsIgnoreCase("left renal pelvis") || part.equalsIgnoreCase("renal pelvis") || part.equalsIgnoreCase("right renal pelvis")
                || part.equalsIgnoreCase("right renal cortex") || part.equalsIgnoreCase("left renal cortex") || part.equalsIgnoreCase("left kidney")
                || part.equalsIgnoreCase("right kidney")){
            exprName = "kidney molecular composition trait";
        }else if(part.equalsIgnoreCase("atrium auricular region") || part.equalsIgnoreCase("heart left ventricle") || part.equalsIgnoreCase("left cardiac atrium") || part.equalsIgnoreCase("mitral valve")
                || part.equalsIgnoreCase("pulmonary valve") || part.equalsIgnoreCase("tricuspid valve") ){
            exprName = "heart molecular composition trait";
        }else if(part.equalsIgnoreCase("C1 segment of cervical spinal cord")){
            exprName = "spinal cord molecular composition trait";
        }else if(part.equalsIgnoreCase("blood") || part.equalsIgnoreCase("umbilical cord blood") || part.equalsIgnoreCase("venous blood") ){
            exprName = "hematopoietic system morphology trait";
        }else if(part.equalsIgnoreCase("EBV-transformed lymphocyte")){
            exprName = "lymphocyte morphology trait";
        }else if(part.equalsIgnoreCase("dura mater")){
            exprName = "meninges morphology trait";
        }else if(part.equalsIgnoreCase("tibial nerve") || part.equalsIgnoreCase("peripheral nervous system")){
            exprName = "nervous system morphology trait";
        }else if(part.equalsIgnoreCase("small intestine Peyers patch")){
            exprName = "Peyers patch morphology trait";
            return exprName;
        }else if(part.equalsIgnoreCase("trachea") || part.equalsIgnoreCase("olfactory apparatus") || part.equalsIgnoreCase("pleura")){
            exprName = "respiratory system morphology trait";
        }else if(part.equalsIgnoreCase("throat") || part.equalsIgnoreCase("chordate pharynx")){
            exprName = "pharynx morphology trait";
        }else if(part.equalsIgnoreCase("distal gut") || part.equalsIgnoreCase("proximal gut") || part.equalsIgnoreCase("terminal ileum") || part.equalsIgnoreCase("ileum")
                || part.equalsIgnoreCase("caecum")){
            exprName = "intestine morphology trait";
        }else if(part.equalsIgnoreCase("oral cavity")){
            exprName = "mouth morphology trait";
        }else if(part.equalsIgnoreCase("cartilage tissue")){
            exprName = "cartilage morphology trait";
        }else if(part.equalsIgnoreCase("bone tissue")){
            exprName = "bone morphology trait";
        }else if(part.equalsIgnoreCase("epithelium of bronchus") || part.equalsIgnoreCase("alveolus of lung")){
            exprName = "lung molecular composition trait";
        }else if(part.equalsIgnoreCase("mucosa")) {
            exprName = "vertebrate trait";
        }else if(part.equalsIgnoreCase("omentum")) {
            exprName = "abdominal wall morphology trait";
        }else if(part.equalsIgnoreCase("pituitary")){
            exprName = "pituitary gland morphology trait";
        }else if(part.equalsIgnoreCase("adrenal")){
            exprName = "adrenal gland molecular composition trait";
        }else if(part.equalsIgnoreCase("thyroid")){
            exprName = "thyroid gland morphology trait";
        }

        String traitId = dao.getTermByTermName(exprName,"VT");
        if(traitId == null)
            exprName = part + " morphology trait";


        return exprName;
    }
    public String getTraitId(String part, String exprName) throws Exception{

        String traitId = null;
        if(exprName.equalsIgnoreCase("Peyers patch morphology trait")) {
            traitId = "VT:0000696";
            return traitId;
        }
        traitId = dao.getTermByTermName(exprName,"VT");
        if(traitId == null)
        {   exprName = part + " morphology trait";
            traitId = dao.getTermByTermName(exprName,"VT");
        }
        return traitId;
    }
    String parseBioSampleId(String sampleRelation) {
        // example:
        // BioSample: https://www.ncbi.nlm.nih.gov/biosample/SAMN02642598||SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX471400
        int doubleBarPos = sampleRelation.indexOf("||");
        if( doubleBarPos<0 ) {
            return "";
        }
        int slashPos = sampleRelation.lastIndexOf('/', doubleBarPos);
        if( slashPos<0 ) {
            return "";
        }
        return sampleRelation.substring(slashPos+1, doubleBarPos);
    }





    int upsertRecord(GeneExpressionRecord rec) throws Exception {
        String sql = "SELECT MAX(gene_expression_exp_record_id) FROM gene_expression_exp_record WHERE experiment_id=? AND sample_id=?";
        int recId = gedao.getCount(sql, rec.getExperimentId(), rec.getSampleId());
        if( recId==0 ) {
            recId = gedao.insertGeneExpressionRecord(rec);
        } else {
            rec.setId(recId);

            // load existing conditions, methods and values
            rec.setValues(gedao.getGeneExpressionRecordValues(recId));
            rec.setConditions(gedao.getConditions(recId));
            rec.setMeasurementMethods(gedao.getMeasurementMethods(recId));

            // since we load new values from disk file, delete the existing values from db
            for( GeneExpressionRecordValue v: rec.getValues() ) {
                v.deleteFlag = true;
            }
        }
        return recId;
    }

    void addConditions( GeneExpressionRecord r ) {
        if( r.getConditions()==null ) {
            r.setConditions(new ArrayList<>());
        }
        if( r.getConditions().isEmpty() ) {
            Condition c = new Condition();
            c.setOrdinality(1);
            c.setGeneExpressionRecordId(r.getId());
            c.setOntologyId("XCO:0000056");
            r.getConditions().add(c);
        }
    }

    void addMeasurementMethods( GeneExpressionRecord r ) {
        if( r.getMeasurementMethods()==null ) {
            r.setMeasurementMethods(new ArrayList<>());
        }
        if( r.getMeasurementMethods().isEmpty() ) {
            MeasurementMethod m = new MeasurementMethod();
            m.setGeneExpressionRecordId(r.getId());
            m.setAccId("MMO:0000659");
            r.getMeasurementMethods().add(m);
        }
    }

    void loadValues(int mapKey, String file) throws Exception {

        FileReader valReader = new FileReader(file);
        BufferedReader tpmsReader = new BufferedReader(valReader);
        List<String> samples = new ArrayList<>();
        String tpmsLine = null;
        //loadedRgdIds = dao.getExistingIds(studyId);
        int count = 0;
        List<GeneExpressionRecordValue> loaded = new ArrayList<>();
        while(( tpmsLine = tpmsReader.readLine() ) != null) {
            if (tpmsLine.startsWith("#")) {
                continue;
            } else {
                String[] cols = tpmsLine.split("[\t]", -1);
                //process header line
                if (count == 0) {
                    for (String col : cols) {
                     samples.add(col);
                    }
                    count ++;
                } else {

                    String ensembleID = cols[0];
                    int rgdId = dao.getRGDIdsByXdbId(20, ensembleID);
                    if (rgdId != 0) {
                        int i = 0;
                        for (String col : cols) {
                            if (!col.isEmpty()) {
                                int j = i - 1;
                                String s = null;
                                if (i != 0 ) {
                                    s = samples.get(j);
                                    GeneExpressionRecordValue rec = new GeneExpressionRecordValue();
                                    rec.setExpressedObjectRgdId(rgdId);

                                    rec.setExpressionValue(Double.parseDouble(col));

                                    rec.setExpressionUnit("TPM");
                                    rec.setExpressionMeasurementAccId(cmoIDs.get(s));

                                    rec.setGeneExpressionRecordId(geneExpressionRecordMap.get(s));
                                    rec.setMapKey(mapKey);
                                    if (rec.getExpressionValue() != 0) {
                                        loaded.add(rec);
                                    }

                                }

                            }

                            i++;
                        }

                        dao.insertGeneExpressionRecordValues(loaded);
                        loaded.clear();
                        System.out.println("Completed rgdId " + rgdId + " " + cols[1]);
                    }
                }
            }
        }
        tpmsReader.close();
    }


}
