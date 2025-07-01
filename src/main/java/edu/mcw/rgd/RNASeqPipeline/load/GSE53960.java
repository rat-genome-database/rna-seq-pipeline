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
import java.util.*;

/**
 * Created by mtutaj on 11/1/2018.
 */
public class GSE53960 {
    GeneExpressionDAO gedao = new GeneExpressionDAO();
    XdbIdDAO xdao = new XdbIdDAO();
    RnaSeqDAO dao = new RnaSeqDAO();
    PhenominerDAO pdao = new PhenominerDAO();

    Map<String, Integer> geneSymbol2RgdIdMap;
    int valueLinesRead = 0;
    int valueLinesLoaded = 0;

    public static void main(String[] args) throws Exception {

        try {
            new GSE53960().run();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    void run() throws Exception {

        String gseAcc = "GSE53960";
        final int STUDY_ID = 3013;

        loadGeneSymbolMap();

        // load all RnaSeq data from DB
        List<RnaSeq> rnaSeqs = dao.getDataForGSE(gseAcc);

        // associate RnaSeq data with experiments
        Map<Experiment, List<RnaSeq>> experimentMap = buildExperimentMap(STUDY_ID, rnaSeqs);

        // process all experiments
        for( Map.Entry<Experiment, List<RnaSeq>> entry: experimentMap.entrySet() ) {
            Experiment e = entry.getKey();
            List<RnaSeq> rnaSeq = entry.getValue();

            for( RnaSeq rs: rnaSeq ) {
                GeneExpressionRecord ger = new GeneExpressionRecord();
                ger.setExperimentId(e.getId());
                ger.setLastModifiedBy("mtutaj");
                ger.setLastModifiedDate(new Date());
                ger.setCurationStatus(20);
                ger.setSpeciesTypeKey(SpeciesType.RAT);

                Sample sample = new Sample();
                parseAge(sample, rs.getSampleAge());
                sample.setNumberOfAnimals(1);

                if( rs.getSampleGender().equals("F") ) {
                    sample.setSex("female");
                } else if( rs.getSampleGender().equals("M") ) {
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
                ger.setSampleId(upsertSample(sample));

                upsertRecord(ger);

                // every record must have a one condition
                addConditions(ger);

                // every record must have a one measurement method
                addMeasurementMethods(ger);

                loadValues(ger, rs.getSampleSupplementaryFiles());

                List<GeneExpressionRecord> records = new ArrayList<>();
                records.add(ger);
                gedao.saveGeneExpressionRecords(records);
            }
        }
    }

    void parseAge(Sample sample, String sampleAge) throws Exception {
        if( sampleAge.endsWith(" weeks") ) {
            String weeks = sampleAge.substring(0, sampleAge.length()-" weeks".length()).trim();
            double ageInDays = 7 * Integer.parseInt(weeks);
            sample.setAgeDaysFromHighBound(ageInDays);
            sample.setAgeDaysFromLowBound(ageInDays);
        } else {
            throw new Exception("unknown sample age: " + sampleAge);
        }
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

    Map<Experiment, List<RnaSeq>> buildExperimentMap(int studyId, List<RnaSeq> rnaSeqs) throws Exception {

        Map<Experiment, List<RnaSeq>> experimentMap = new HashMap<>();

        List<Experiment> experiments = pdao.getExperiments(studyId);

        for( RnaSeq r: rnaSeqs ) {

            // match 4-letters of experiment name with SAMPLE_SOURCE
            String key = r.getSampleSource().toLowerCase().substring(0, 4);
            Experiment expInRgd = null;
            for( Experiment e: experiments ) {
                if( e.getName().startsWith(key) ) {
                    expInRgd = e;
                    break;
                }
            }
            if( expInRgd==null ) {
                throw new Exception("ERROR: no matching experiment in RGD");
            }

            List<RnaSeq> rnaSeqsInExp = experimentMap.get(expInRgd);
            if( rnaSeqsInExp==null ) {
                rnaSeqsInExp = new ArrayList<>();
                experimentMap.put(expInRgd, rnaSeqsInExp);
            }
            rnaSeqsInExp.add(r);
        }
        return experimentMap;
    }

    int upsertSample(Sample s) throws Exception {
        String sql = "SELECT MAX(sample_id) FROM sample WHERE geo_sample_acc=?";
        int sampleId = pdao.getCount(sql, s.getGeoSampleAcc());
        if( sampleId==0 ) {
            sampleId = pdao.insertSample(s);
        } else {
            s.setId(sampleId);
        }
        return sampleId;
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

    void loadValues(GeneExpressionRecord rec, String sampleSupplementaryFiles) throws Exception {
        List<GeneExpressionRecordValue> values = rec.getValues();
        if( values==null ) {
            values = new ArrayList<>();
            rec.setValues(values);
        }

        int splitPos = sampleSupplementaryFiles.indexOf("||");
        if( splitPos>0 ) {
            String fileUrl = sampleSupplementaryFiles.substring(0, splitPos);
            int slashPos = fileUrl.lastIndexOf('/');
            String fileName = fileUrl.substring(slashPos+1);
            System.out.println(fileUrl);

            FileDownloader fd = new FileDownloader();
            fd.setExternalFile(fileUrl);
            fd.setLocalFile("data/"+fileName);
            fd.setUseCompression(true);
            String localFile = fd.downloadNew();

            BufferedReader br = Utils.openReader(localFile);
            String line;
            while( (line=br.readLine())!=null ) {
                valueLinesRead++;

                String[] cols = line.split("[\\t]", -1);
                String geneSymbol = cols[0];
                String value = cols[1];
                Integer geneRgdId = geneSymbol2RgdIdMap.get(geneSymbol);
                if( geneRgdId!=null ) {
                    valueLinesLoaded++;

                    // lookup for a record with a gene, and update the value
                    GeneExpressionRecordValue val = null;
                    for( GeneExpressionRecordValue v: rec.getValues() ) {
                        if( v.getExpressedObjectRgdId()==geneRgdId ) {
                            // found it!
                            val = v;
                            val.deleteFlag = false;
                            break;
                        }
                    }

                    if( val==null ) {
                        val = new GeneExpressionRecordValue();
                        val.setGeneExpressionRecordId(rec.getId());
                        val.setExpressedObjectRgdId(geneRgdId);
                        val.setExpressionMeasurementAccId("CMO:0001919");
                        val.setExpressionUnit("FPKM");
                        val.setMapKey(60);
                        rec.getValues().add(val);
                    }
                    Double dval = Double.parseDouble(value);
                    val.setExpressionValue(dval);
                }
            }
            br.close();
        }
    }

    // example:
    //# mRNA 	AceView Gene 	Entrez Gene ID (if any)  	included RefSeq (if any)
    //"Abca15.aSep08"	"Abca15"	"293442"	"NM_001106293"
    //
    void loadGeneSymbolMap() throws Exception {
        Map<String,String> geneSymbol2NcbiGeneIdMap = new HashMap<>();

        int linesProcessed = 0;
        String fname = "data/AceView.ncbi_4.mRNA2GeneID2NM.txt.gz";
        BufferedReader in = Utils.openReader(fname);
        String line;
        while( (line=in.readLine())!=null ) {
            linesProcessed++;
            if( line.startsWith("#") ) {
                continue;
            }
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<4 ) {
                continue;
            }

            String geneSymbol = cols[1];
            if( geneSymbol.startsWith("\"") && geneSymbol.endsWith("\"") ) {
                geneSymbol = geneSymbol.substring(1, geneSymbol.length()-1 );
            }

            String ncbiGeneId = cols[2];
            if( ncbiGeneId.startsWith("\"") && ncbiGeneId.endsWith("\"") ) {
                ncbiGeneId = ncbiGeneId.substring(1, ncbiGeneId.length()-1 );
            }
            geneSymbol2NcbiGeneIdMap.put(geneSymbol, ncbiGeneId);
        }
        in.close();

        System.out.println("lines processed "+linesProcessed);
        System.out.println("loaded "+geneSymbol2NcbiGeneIdMap.size()+" gene symbols mapped to NCBI gene ids");


        // resolve NCBI gene ids
        geneSymbol2RgdIdMap = new HashMap<>();

        Map<String, Integer> ncbiGeneId2geneRgdIdMap = new HashMap<>();
        List<Gene> genes = new GeneDAO().getActiveGenes(SpeciesType.RAT);
        System.out.println("  loaded active rat genes excluding variants "+genes.size());
        Set<Integer> geneRgdIds = new HashSet<>(); // rgd ids for active rat genes, excluding splices and alleles
        for( Gene g: genes ) {
            geneRgdIds.add(g.getRgdId());
        }

        List<XdbId> xdbIds = getActiveXdbIds(XdbId.XDB_KEY_ENTREZGENE, RgdId.OBJECT_KEY_GENES, SpeciesType.RAT);
        System.out.println("  loaded ncbi gene ids for rat "+xdbIds.size());
        for( XdbId id: xdbIds ) {
            if( geneRgdIds.contains(id.getRgdId()) ) {
                ncbiGeneId2geneRgdIdMap.put(id.getAccId(), id.getRgdId());
            }
        }

        for( Map.Entry<String,String> entry: geneSymbol2NcbiGeneIdMap.entrySet() ) {
            String geneId = entry.getValue();
            Integer matchingGeneRgdId = ncbiGeneId2geneRgdIdMap.get(geneId);
            if( matchingGeneRgdId!=null ) {
                geneSymbol2RgdIdMap.put(entry.getKey(), matchingGeneRgdId);
            }
        }
        System.out.println("resolved "+geneSymbol2RgdIdMap.size()+" NCBI gene ids to gene RGD IDs");
    }

    public List<XdbId> getActiveXdbIds(int xdbKey, int objectKey, int speciesTypeKey) throws Exception {

        String sql = "SELECT x.* FROM rgd_acc_xdb x,rgd_ids r "+
                "WHERE x.xdb_key=? AND object_key=? AND object_status='ACTIVE' AND x.rgd_id=r.rgd_id AND species_type_key=?";

        return xdao.executeXdbIdQuery(sql, xdbKey, objectKey, speciesTypeKey);
    }
}
