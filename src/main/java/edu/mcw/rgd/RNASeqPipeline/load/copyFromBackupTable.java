package edu.mcw.rgd.RNASeqPipeline.load;

import edu.mcw.rgd.dao.DataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class copyFromBackupTable {

    public static void  main(String[] args) throws Exception {

        Connection conn = DataSourceFactory.getInstance().getDataSource().getConnection();
        String sql1 = "SELECT geo_accession_id,platform_id,sample_accession_id,pubmed_id,rgd_tissue_term_acc,rgd_cell_term_acc,rgd_strain_term_acc,"+
                "rgd_strain_rgd_id,curation_status,created_in_rgd,date_mapped FROM rna_seq_bak";

        String sql2 = "UPDATE rna_seq SET pubmed_id=?,rgd_tissue_term_acc=?,rgd_cell_term_acc=?,rgd_strain_term_acc=?,"+
                "rgd_strain_rgd_id=?,curation_status=?,created_in_rgd=?,date_mapped=? " +
                "WHERE geo_accession_id=? AND platform_id=? AND sample_accession_id=?";

        PreparedStatement ps2 = conn.prepareStatement(sql2);
        PreparedStatement ps1 = conn.prepareStatement(sql1);
        ResultSet rs = ps1.executeQuery();
        while( rs.next() ) {
            String geoAccId = rs.getString(1);
            String platformId = rs.getString(2);
            String sampleAccId = rs.getString(3);
            String pubmedId = rs.getString(4);
            String rgd_tissue_term_acc = rs.getString(5);
            String rgd_cell_term_acc = rs.getString(6);
            String rgd_strain_term_acc = rs.getString(7);
            String rgd_strain_rgd_id = rs.getString(8);
            String curation_status = rs.getString(9);
            java.sql.Date created_in_rgd = rs.getDate(10);
            java.sql.Date date_mapped = rs.getDate(11);

            ps2.setString(1, pubmedId);
            ps2.setString(2, rgd_tissue_term_acc);
            ps2.setString(3, rgd_cell_term_acc);
            ps2.setString(4, rgd_strain_term_acc);
            ps2.setString(5, rgd_strain_rgd_id);
            ps2.setString(6, curation_status);
            ps2.setDate(7, created_in_rgd);
            ps2.setDate(8, date_mapped);

            ps2.setString(9, geoAccId);
            ps2.setString(10, platformId);
            ps2.setString(11, sampleAccId);
            int r = ps2.executeUpdate();
            System.out.println(r+" "+geoAccId+" "+platformId+" "+sampleAccId);
        }
        conn.close();
    }
}
