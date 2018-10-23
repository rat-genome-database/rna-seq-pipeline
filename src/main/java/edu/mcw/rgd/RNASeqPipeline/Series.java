package edu.mcw.rgd.RNASeqPipeline;

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


    public void handleSeries(String line) {
        /*if (line.startsWith("^SERIES")) {
            addSeries();
            if (SoftFileParser.DEBUG) System.out.println("Series: " + line.replace("^SERIES = ", ""));
        } else {*/
        // Semiautomatically generated code
        if (line.startsWith("!Series_title")) {
            title = line.replace("!Series_title = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series title: " + title);
        } else if (line.startsWith("!Series_geo_accession")) {
            geoAccessionID = line.replace("!Series_geo_accession = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series geo accession: " + geoAccessionID);
        } else if (line.startsWith("!Series_status")) {
            status = line.replace("!Series_status = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series status: " + status);
        } else if (line.startsWith("!Series_submission_date")) {
            submissionDate = line.replace("!Series_submission_date = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series submission date: " + submissionDate);
        } else if (line.startsWith("!Series_last_update_date")) {
            lastUpdateDate = line.replace("!Series_last_update_date = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series last update date: " + lastUpdateDate);
        } else if (line.startsWith("!Series_pubmed_id")) {
            pubmedID = line.replace("!Series_pubmed_id = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series pubmed id: " + pubmedID);
        } else if (line.startsWith("!Series_summary")) {
            summary = line.replace("!Series_summary = ", "").replace(" ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series summary: " + summary);
        } else if (line.startsWith("!Series_type")) {
            type = line.replace("!Series_type = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series type: " + type);
        }

        // Manual handling of Subtypes
        else if (line.startsWith("!Series_overall_design")) {
            overallDesign.addStore(line, "!Series_overall_design = ");
            if (SoftFileParser.DEBUG)
                System.out.println("Series overall design: " + overallDesign.getCurrentStore());
        } else if (line.startsWith("!Series_contributor")) {
            contributor.addStore(line, "!Series_contributor = ");
            if (SoftFileParser.DEBUG)
                System.out.println("Series contributor: " + contributor.getCurrentStore());
        } else if (line.startsWith("!Series_sample_id")) {
            sampleID.addStore(line, "!Series_sample_id = ");
            if (SoftFileParser.DEBUG)
                System.out.println("Series sample ID: " + sampleID.getCurrentStore());
        } else if (line.startsWith("!Series_supplementary_file")) {
            supplementaryFile.addStore(line, "!Series_supplementary_file = ");
            if (SoftFileParser.DEBUG)
                System.out.println("Series supplementary file: " + supplementaryFile.getCurrentStore());
        } else if (line.startsWith("!Series_relation")) {
            relation.addStore(line, "!Series_relation = ");
            if (SoftFileParser.DEBUG)
                System.out.println("Series relation: " + relation.getCurrentStore());
        }

        // Semiautomatically generated code
        else if (line.startsWith("!Series_contact_name")) {
            contactName = line.replace("!Series_contact_name = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact name: " + contactName);
        } else if (line.startsWith("!Series_contact_email")) {
            contactEmail = line.replace("!Series_contact_email = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact email: " + contactEmail);
        } else if (line.startsWith("!Series_contact_phone")) {
            contactPhone = line.replace("!Series_contact_phone = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact phone: " + contactPhone);
        } else if (line.startsWith("!Series_contact_fax")) {
            contactFax = line.replace("!Series_contact_fax = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact fax: " + contactFax);
        } else if (line.startsWith("!Series_contact_institute")) {
            contactInstitution = line.replace("!Series_contact_institute = ", "");
            if (SoftFileParser.DEBUG)
                System.out.println("Series contact institute: " + contactInstitution);
        } else if (line.startsWith("!Series_contact_department")) {
            contactDepartment = line.replace("!Series_contact_department = ", "");
            if (SoftFileParser.DEBUG)
                System.out.println("Series contact department: " + contactDepartment);
        } else if (line.startsWith("!Series_contact_laboratory")) {
            contactLaboratory = line.replace("!Series_contact_laboratory = ", "");
            if (SoftFileParser.DEBUG)
                System.out.println("Series contact laboratory: " + contactLaboratory);
        } else if (line.startsWith("!Series_contact_address")) {
            contactAddress = line.replace("!Series_contact_address = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact address: " + contactAddress);
        } else if (line.startsWith("!Series_contact_city")) {
            contactCity = line.replace("!Series_contact_city = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact city: " + contactCity);
        } else if (line.startsWith("!Series_contact_state")) {
            contactState = line.replace("!Series_contact_state = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact state: " + contactState);
        } else if (line.startsWith("!Series_contact_zip/postal_code")) {
            contactZipcode = line.replace("!Series_contact_zip/postal_code = ", "");
            if (SoftFileParser.DEBUG)
                System.out.println("Series contact zip/postal code: " + contactZipcode);
        } else if (line.startsWith("!Series_contact_country")) {
            contactCountry = line.replace("!Series_contact_country = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact country: " + contactCountry);
        } else if (line.startsWith("!Series_contact_web_link")) {
            contactWebLink = line.replace("!Series_contact_web_link = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series contact web link: " + contactWebLink);
        } else if (line.startsWith("!Series_platform_id")) {
            platformID = line.replace("!Series_platform_id = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series platform id: " + platformID);
        } else if (line.startsWith("!Series_platform_organism")) {
            platformOrganism = line.replace("!Series_platform_organism = ", "");
            if (SoftFileParser.DEBUG)
                System.out.println("Series platform organism: " + platformOrganism);
        } else if (line.startsWith("!Series_platform_taxid")) {
            platformTaxID = line.replace("!Series_platform_taxid = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series platform taxid: " + platformTaxID);
        } else if (line.startsWith("!Series_sample_organism")) {
            sampleOrganism = line.replace("!Series_sample_organism = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series sample organism: " + sampleOrganism);
        } else if (line.startsWith("!Series_sample_taxid")) {
            sampleTaxID = line.replace("!Series_sample_taxid = ", "");
            if (SoftFileParser.DEBUG) System.out.println("Series sample taxid: " + sampleTaxID);
        } else {
            if (SoftFileParser.DEBUG) System.out.println(line);
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
