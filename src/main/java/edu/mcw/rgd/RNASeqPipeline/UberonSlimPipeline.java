package edu.mcw.rgd.RNASeqPipeline;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.process.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UberonSlimPipeline {

    static String[] uberonAccessions = {
            "UBERON:0001013",
            "UBERON:0005409",
            "UBERON:0000026",
            "UBERON:0001009",
            "UBERON:0000924",
            "UBERON:0000949",
            "UBERON:0000925",
            "UBERON:0016687",
            "UBERON:0002330",
            "UBERON:0002193",
            "UBERON:0002423",
            "UBERON:0002416",
            "UBERON:0003104",
            "UBERON:0000926",
            "UBERON:0002204",
            "UBERON:0001016",
            "UBERON:0001008",
            "UBERON:0000990",
            "UBERON:0001004",
            "UBERON:0001032",
            "UBERON:0002104",
            };

    public static void main( String[] args ) throws Exception {

        String sql1 = "DELETE FROM ont_dag_uberon_slim";
        AbstractDAO dao = new AbstractDAO();
        int rowsDeleted = dao.update(sql1);
        System.out.println("rows deleted from ONT_DAG_UBERON_SLIM: "+rowsDeleted);

        String sql2 = """
            INSERT INTO ont_dag_uberon_slim (
            SELECT ?,child_term_acc,SYSDATE,SYSDATE FROM(
            SELECT child_term_acc
            FROM ont_dag
            START WITH parent_term_acc = ?
            CONNECT BY PRIOR child_term_acc = parent_term_acc
             INTERSECT
            SELECT tissue_ont_id FROM sample
            )
            )
            """;

        for( String uberonAcc: uberonAccessions ) {
            dao.update(sql2, uberonAcc, uberonAcc);
        }

        int rowsInserted = dao.getCount("SELECT COUNT(*) FROM ont_dag_uberon_slim");
        System.out.println("rows inserted to ONT_DAG_UBERON_SLIM: "+rowsInserted);


        System.out.println("=== OK ===");
    }
}
