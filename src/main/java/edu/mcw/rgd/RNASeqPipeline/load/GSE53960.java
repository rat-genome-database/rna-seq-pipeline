package edu.mcw.rgd.RNASeqPipeline.load;

import edu.mcw.rgd.dao.impl.GeneExpressionDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.datamodel.pheno.Condition;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecord;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecordValue;
import edu.mcw.rgd.datamodel.pheno.Sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created by mtutaj on 11/1/2018.
 */
public class GSE53960 {
    GeneExpressionDAO dao = new GeneExpressionDAO();
    XdbIdDAO xdao = new XdbIdDAO();

    public static void main(String[] args) throws Exception {

        try {
            new GSE53960().run();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    void run() throws Exception {

        String gseAcc = "GSE53960";
        int studyId = 3013;

        // load all experiment records
        String sql = "SELECT * FROM rna_seq WHERE geo_accession_id=?";

        /*
        int eid = 6796; // experiment id

        int linesWithUnresolvedGenes = 0;
        int linesWithResolvedGenes = 0;

        String fileName = "/data/gse/GSE50027_FPKM.txt";
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        String header = in.readLine();
        String[] headerCols = header.split("[\\t]", -1);
        int[] sampleIds = getSampleIds(headerCols);
        Map<Integer, GeneExpressionRecord> records = loadRecords(eid); // records: key=sample_id

        // generate record
        String line;
        while( (line=in.readLine())!=null ) {
            String[] cols = line.split("[\\t]", -1);
            String ensemblGeneId =  cols[0];
            Gene gene = matchGene(ensemblGeneId);
            if( gene==null ) {
                linesWithUnresolvedGenes++;
                continue;
            }
            linesWithResolvedGenes++;

            // process only those columns having assigned a sample id
            for( int col=1; col<cols.length; col++ ) {
                if( sampleIds[col]==0 ) {
                    continue;
                }
                addValue(sampleIds[col], cols[col], gene.getRgdId(), eid, records);
            }
        }
        in.close();

        // every record must have a one condition
        addConditions(records.values());

        // every record must have a one measurement method
        addMeasurementMethods(records.values());

        dao.saveGeneExpressionRecords(records.values());

        System.out.println("Lines with unresolved genes: "+linesWithUnresolvedGenes);
        System.out.println("Lines with resolved genes: "+linesWithResolvedGenes);*/
    }
/*
    int[] getSampleIds() throws Exception {
        String fileName = "/data/gse/GSE53960_samples.txt";
        BufferedReader in = new BufferedReader(new FileReader(fileName));

        String line;
        while( (line=in.readLine())!=null ) {
            String[] cols = line.split("[\\t]", -1);
            String geoSampleAcc = cols[0];
            String geoSampleName = cols[1];

            edu.mcw.rgd.datamodel.pheno.Sample s = new Sample();
            int sampleId = getSampleId(geoSampleAcc);
            if( sampleId==0 ) {

                // geoSampleName: SEQC_Lvr_F_006_2
                //           tissue=Liver  sex=Female  animal_age=6 weeks
                String[] words = geoSampleName.split("_", -1);
                String sex;
                switch( words[2] ) {
                    case "F": sex = "female"; break;
                    case "M": sex = "male"; break;
                    default: sex = null; break;
                }

                String tissue;
                switch( words[1] ) {
                    case "Adr": tissue = "UBERON:0002369"; break;
                    case "Brn": tissue = "UBERON:0000955"; break;
                    case "Hrt": tissue = "UBERON:0000948"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;
                    case "Lng": tissue = "UBERON:0002113"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;
                    case "Kdn": tissue = "UBERON:0002113"; break;

                    default: tissue = null; break;
                }

                s.setAgeDaysFromHighBound(105);
                s.setAgeDaysFromLowBound(105);
                s.setNumberOfAnimals(1);
                s.setNotes(col);
                s.setSex(sex);
                s.setStrainAccId("RS:0004581");
                s.setTissueAccId("VT:0010497");
                s.setGeoSampleAcc(geoSampleAcc);
                sampleId = dao.insertSample(s);
                System.out.println(col + " " + s.getGeoSampleAcc() + " " + sampleId);
            }
            sampleIds[i] = sampleId;
        }
        in.close();

        final String strainAccId = "RS:0004581";
        int[] sampleIds = new int[geoSampleMap.size()];
        int col = 0;
        for( Map.Entry<String,String> entry: geoSampleMap.entrySet() ) {
            String geoSampleAcc = entry.;

        }
        return sampleIds;
    }

    int getSampleId(String geoSampleAcc) throws Exception {
        String sql = "SELECT sample_id FROM sample WHERE geo_sample_acc=?";
        return dao.getCount(sql, geoSampleAcc);
    }
    */
/*

    Gene matchGene(String ensemblGeneId) throws Exception {
        List<Gene> genes = xdao.getGenesByXdbId(XdbId.XDB_KEY_ENSEMBL_GENES, ensemblGeneId) ;
        if( genes.isEmpty() ) {
            System.out.println(" === no match for "+ensemblGeneId);
            return null;
        }
        if( genes.size()>1 ) {
            System.out.println(" === multi match for "+ensemblGeneId);
            return null;
        }
        return genes.get(0);
    }

    Map<Integer, GeneExpressionRecord> loadRecords(int experimentId) throws Exception {

        List<GeneExpressionRecord> records = dao.getGeneExpressionRecords(experimentId);
        Map<Integer, GeneExpressionRecord> map = new HashMap<>();
        for( GeneExpressionRecord r: records ) {
            GeneExpressionRecord prevRec = map.put(r.getSampleId(), r);
            if( prevRec!=null ) {
                throw new Exception("problem");
            }
            for( GeneExpressionRecordValue v: r.getValues() ) {
                // when loading from db, we set the values to be deleted;
                // loading the incoming data will clear the flag, effectively leaving for delete only the stale values
                v.deleteFlag = true;
            }
        }
        return map;
    }

    void addValue(int sampleId, String value, int geneRgdId, int experimentId, Map<Integer, GeneExpressionRecord> records) {
        // there must be an experiment record with a sample_id
        GeneExpressionRecord rec = records.get(sampleId);
        if( rec==null ) {
            // create new record
            rec = new GeneExpressionRecord();
            rec.setExperimentId(experimentId);
            rec.setSampleId(sampleId);
            rec.setLastModifiedBy("mtutaj");
            rec.setLastModifiedDate(new Date());
            rec.setCurationStatus(20);
            rec.setSpeciesTypeKey(3);
            records.put(sampleId, rec);

            rec.setValues(new ArrayList<>());
        }

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

    void addConditions( Collection<GeneExpressionRecord> records ) {
        for( GeneExpressionRecord r: records ) {
            if( r.getConditions().isEmpty() ) {
                Condition c = new Condition();
                c.setOrdinality(1);
                c.setGeneExpressionRecordId(r.getId());
                c.setOntologyId("XCO:0000056");
                r.getConditions().add(c);
            }
        }
    }

    void addMeasurementMethods( Collection<GeneExpressionRecord> records ) {
        for( GeneExpressionRecord r: records ) {
            if( r.getMeasurementMethods().isEmpty() ) {
                MeasurementMethod m = new MeasurementMethod();
                m.setGeneExpressionRecordId(r.getId());
                m.setAccId("MMO:0000659");
                r.getMeasurementMethods().add(m);
            }
        }
    }
*/
}
