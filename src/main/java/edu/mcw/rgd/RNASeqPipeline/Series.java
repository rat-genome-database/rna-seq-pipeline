package edu.mcw.rgd.RNASeqPipeline;

import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class Series {


    private String title, geoAccessionID, status,
            submissionDate, lastUpdateDate, pubmedID,
            summary, type;

    private Subtype overallDesign = new Subtype(), contributor = new Subtype(),
            sampleID = new Subtype(), supplementaryFile = new Subtype(),
            relation = new Subtype();

    private String contactName, contactEmail, contactPhone, contactFax,
            contactInstitution, contactDepartment, contactLaboratory,
            contactAddress, contactCity, contactState,
            contactZipcode, contactCountry, contactWebLink;

    private String platformID, platformOrganism, platformTaxID,
            sampleOrganism, sampleTaxID;


    private List<Platform> platformList = new ArrayList<Platform>(0);

    private List<Sample> sampleList = new ArrayList<Sample>(0);


    public void handleSeries(String line, Logger log) {
        /*if (line.startsWith("^SERIES")) {
            addSeries();
            if (SoftFileParser.DEBUG) System.out.println("Series: " + line.replace("^SERIES = ", ""));
        } else {*/
        // Semiautomatically generated code
        if (line.startsWith("!Series_title")) {
            title = line.replace("!Series_title = ", "");
            log.debug("Series title: " + title);
        } else if (line.startsWith("!Series_geo_accession")) {
            geoAccessionID = line.replace("!Series_geo_accession = ", "");
            log.debug("Series geo accession: " + geoAccessionID);
        } else if (line.startsWith("!Series_status")) {
            status = line.replace("!Series_status = ", "");
            log.debug("Series status: " + status);
        } else if (line.startsWith("!Series_submission_date")) {
            submissionDate = line.replace("!Series_submission_date = ", "");
            log.debug("Series submission date: " + submissionDate);
        } else if (line.startsWith("!Series_last_update_date")) {
            lastUpdateDate = line.replace("!Series_last_update_date = ", "");
            log.debug("Series last update date: " + lastUpdateDate);
        } else if (line.startsWith("!Series_pubmed_id")) {
            pubmedID = line.replace("!Series_pubmed_id = ", "");
            log.debug("Series pubmed id: " + pubmedID);
        } else if (line.startsWith("!Series_summary")) {
            summary = line.replace("!Series_summary = ", "").replace(" ", "");
            log.debug("Series summary: " + summary);
        } else if (line.startsWith("!Series_type")) {
            type = line.replace("!Series_type = ", "");
            log.debug("Series type: " + type);
        }

        // Manual handling of Subtypes
        else if (line.startsWith("!Series_overall_design")) {
            overallDesign.addStore(line, "!Series_overall_design = ");
            log.debug("Series overall design: " + overallDesign.getCurrentStore());
        } else if (line.startsWith("!Series_contributor")) {
            contributor.addStore(line, "!Series_contributor = ");
            log.debug("Series contributor: " + contributor.getCurrentStore());
        } else if (line.startsWith("!Series_sample_id")) {
            sampleID.addStore(line, "!Series_sample_id = ");
            log.debug("Series sample ID: " + sampleID.getCurrentStore());
        } else if (line.startsWith("!Series_supplementary_file")) {
            supplementaryFile.addStore(line, "!Series_supplementary_file = ");
            log.debug("Series supplementary file: " + supplementaryFile.getCurrentStore());
        } else if (line.startsWith("!Series_relation")) {
            relation.addStore(line, "!Series_relation = ");
            log.debug("Series relation: " + relation.getCurrentStore());
        }

        // Semiautomatically generated code
        else if (line.startsWith("!Series_contact_name")) {
            contactName = line.replace("!Series_contact_name = ", "");
            log.debug("Series contact name: " + contactName);
        } else if (line.startsWith("!Series_contact_email")) {
            contactEmail = line.replace("!Series_contact_email = ", "");
            log.debug("Series contact email: " + contactEmail);
        } else if (line.startsWith("!Series_contact_phone")) {
            contactPhone = line.replace("!Series_contact_phone = ", "");
            log.debug("Series contact phone: " + contactPhone);
        } else if (line.startsWith("!Series_contact_fax")) {
            contactFax = line.replace("!Series_contact_fax = ", "");
            log.debug("Series contact fax: " + contactFax);
        } else if (line.startsWith("!Series_contact_institute")) {
            contactInstitution = line.replace("!Series_contact_institute = ", "");
            log.debug("Series contact institute: " + contactInstitution);
        } else if (line.startsWith("!Series_contact_department")) {
            contactDepartment = line.replace("!Series_contact_department = ", "");
            log.debug("Series contact department: " + contactDepartment);
        } else if (line.startsWith("!Series_contact_laboratory")) {
            contactLaboratory = line.replace("!Series_contact_laboratory = ", "");
            log.debug("Series contact laboratory: " + contactLaboratory);
        } else if (line.startsWith("!Series_contact_address")) {
            contactAddress = line.replace("!Series_contact_address = ", "");
            log.debug("Series contact address: " + contactAddress);
        } else if (line.startsWith("!Series_contact_city")) {
            contactCity = line.replace("!Series_contact_city = ", "");
            log.debug("Series contact city: " + contactCity);
        } else if (line.startsWith("!Series_contact_state")) {
            contactState = line.replace("!Series_contact_state = ", "");
            log.debug("Series contact state: " + contactState);
        } else if (line.startsWith("!Series_contact_zip/postal_code")) {
            contactZipcode = line.replace("!Series_contact_zip/postal_code = ", "");
            log.debug("Series contact zip/postal code: " + contactZipcode);
        } else if (line.startsWith("!Series_contact_country")) {
            contactCountry = line.replace("!Series_contact_country = ", "");
            log.debug("Series contact country: " + contactCountry);
        } else if (line.startsWith("!Series_contact_web_link")) {
            contactWebLink = line.replace("!Series_contact_web_link = ", "");
            log.debug("Series contact web link: " + contactWebLink);
        } else if (line.startsWith("!Series_platform_id")) {
            platformID = line.replace("!Series_platform_id = ", "");
            log.debug("Series platform id: " + platformID);
        } else if (line.startsWith("!Series_platform_organism")) {
            platformOrganism = line.replace("!Series_platform_organism = ", "");
            log.debug("Series platform organism: " + platformOrganism);
        } else if (line.startsWith("!Series_platform_taxid")) {
            platformTaxID = line.replace("!Series_platform_taxid = ", "");
            log.debug("Series platform taxid: " + platformTaxID);
        } else if (line.startsWith("!Series_sample_organism")) {
            sampleOrganism = line.replace("!Series_sample_organism = ", "");
            log.debug("Series sample organism: " + sampleOrganism);
        } else if (line.startsWith("!Series_sample_taxid")) {
            sampleTaxID = line.replace("!Series_sample_taxid = ", "");
            log.debug("Series sample taxid: " + sampleTaxID);
        } else {
            log.debug(line);
        }

    }

     public int getNumRatSamples() {
        int count = 0;

        for (int i = 0; i < this.sampleList.size(); i++) {
            Sample sample = this.sampleList.get(i);

            if (sample.getOrganism_ch1().equals("Rattus norvegicus")) count += 1;
        }

        return count;
    }

    public String findPlatformName(String samplePlatformID) {
        Platform platform;
        int i;

        for (i = 0; i < this.platformList.size(); i++) {
            platform = this.platformList.get(i);

            if (platform.getGeoAccessionID().equals(samplePlatformID)) return platform.getTitle();
        }

        return "";
    }
    public Platform getLastPlatform() {
        return platformList.get(platformList.size()-1);
    }

    public Sample getLastSample() {
        return sampleList.get(sampleList.size()-1);
    }

    // End of printSeriesAndSamples()
    public String getTitle() {
        return title;
    }

    public String getGeoAccessionID() {
        return geoAccessionID;
    }

    public String getStatus() {
        return status;
    }

    public String getSubmissionDate() {
        return submissionDate;
    }

    public String getLastUpdateDate() {
        return lastUpdateDate;
    }

    public String getPubmedID() {
        return pubmedID;
    }

    public String getSummary() {
        return summary;
    }

    public String getType() {
        return type;
    }

    public Subtype getOverallDesign() {
        return overallDesign;
    }

    public Subtype getContributor() {
        return contributor;
    }

    public Subtype getSampleID() {
        return sampleID;
    }

    public Subtype getSupplementaryFile() {
        return supplementaryFile;
    }

    public Subtype getRelation() {
        return relation;
    }

    public String getContactName() {
        return contactName;
    }

    public String getContactEmail() {
        return contactEmail;
    }

    public String getContactPhone() {
        return contactPhone;
    }

    public String getContactFax() {
        return contactFax;
    }

    public String getContactInstitution() {
        return contactInstitution;
    }

    public String getContactDepartment() {
        return contactDepartment;
    }

    public String getContactLaboratory() {
        return contactLaboratory;
    }

    public String getContactAddress() {
        return contactAddress;
    }

    public String getContactCity() {
        return contactCity;
    }

    public String getContactState() {
        return contactState;
    }

    public String getContactZipcode() {
        return contactZipcode;
    }

    public String getContactCountry() {
        return contactCountry;
    }

    public String getContactWebLink() {
        return contactWebLink;
    }

    public String getPlatformID() {
        return platformID;
    }

    public String getPlatformOrganism() {
        return platformOrganism;
    }

    public String getPlatformTaxID() {
        return platformTaxID;
    }

    public String getSampleOrganism() {
        return sampleOrganism;
    }

    public String getSampleTaxID() {
        return sampleTaxID;
    }
    public List<Platform> getPlatformList() {
        return platformList;
    }

    public List<Sample> getSampleList() {
        return sampleList;
    }


}
