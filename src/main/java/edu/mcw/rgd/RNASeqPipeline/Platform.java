package edu.mcw.rgd.RNASeqPipeline;

public class Platform {

    private String title;
    private String geoAccessionID;
    private String status;
    private String submissionDate;
    private String lastUpdateDate;
    private String technology;
    private String distribution;
    private String organism;
    private String taxID;
    private String dataRowCount;

    private String contactName, contactEmail, contactPhone, contactFax,
            contactAddress, contactCity, contactState, contactZipcode, contactCountry,
            contactLaboratory, contactDepartment, contactInstitution, contactWebLink;

    private Subtype contributor = new Subtype(), description = new Subtype(),
            catalogNumber = new Subtype(), manufactureProtocol = new Subtype(),
            manufacturer = new Subtype(), relation = new Subtype(),
            supplementaryFile = new Subtype(), support = new Subtype(),
            webLink = new Subtype();

    

    public void handlePlatform(String line) {
        /*if (line.startsWith("^PLATFORM")) {
            addPlatform();
            if (SoftFileParser.DEBUG) System.out.println("Platform: " + line.replace("^PLATFORM = ", ""));
        } else {*/
            if (line.startsWith("!Platform_title")) {
                title = line.replace("!Platform_title = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform title: " + title);
            } else if (line.startsWith("!Platform_geo_accession")) {
                geoAccessionID = line.replace("!Platform_geo_accession = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform geo accession: " + geoAccessionID);
            } else if (line.startsWith("!Platform_status")) {
                status = line.replace("!Platform_status = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform status: " + status);
            } else if (line.startsWith("!Platform_submission_date")) {
                submissionDate = line.replace("!Platform_submission_date = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform submission date: " + submissionDate);
            } else if (line.startsWith("!Platform_last_update_date")) {
                lastUpdateDate = line.replace("!Platform_last_update_date = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform last update date: " + lastUpdateDate);
            } else if (line.startsWith("!Platform_technology")) {
                technology = line.replace("!Platform_technology = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform technology: " + technology);
            } else if (line.startsWith("!Platform_distribution")) {
                distribution = line.replace("!Platform_distribution = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform distribution: " + distribution);
            } else if (line.startsWith("!Platform_organism")) {
                organism = line.replace("!Platform_organism = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform organism: " + organism);
            } else if (line.startsWith("!Platform_taxid")) {
                taxID = line.replace("!Platform_taxid = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform taxid: " + taxID);
            } else if (line.startsWith("!Platform_data_row_count")) {
                dataRowCount = line.replace("!Platform_data_row_count = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform data row count: " + dataRowCount);
            } else if (line.startsWith("!Platform_contact_name")) {
                contactName = line.replace("!Platform_contact_name = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform contact name: " + contactName);
            } else if (line.startsWith("!Platform_contact_email")) {
                contactEmail = line.replace("!Platform_contact_email = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform contact email: " + contactEmail);
            } else if (line.startsWith("!Platform_contact_phone")) {
                contactPhone = line.replace("!Platform_contact_phone = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform contact phone: " + contactPhone);
            } else if (line.startsWith("!Platform_contact_fax")) {
                contactFax = line.replace("!Platform_contact_fax = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform contact fax: " + contactFax);
            } else if (line.startsWith("!Platform_contact_address")) {
                contactAddress = line.replace("!Platform_contact_address = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact address: " + contactAddress);
            } else if (line.startsWith("!Platform_contact_city")) {
                contactCity = line.replace("!Platform_contact_city = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform contact city: " + contactCity);
            } else if (line.startsWith("!Platform_contact_state")) {
                contactState = line.replace("!Platform_contact_state = ", "");
                if (SoftFileParser.DEBUG) System.out.println("Platform contact state: " + contactState);
            } else if (line.startsWith("!Platform_contact_zip/postal_code")) {
                contactZipcode = line.replace("!Platform_contact_zip/postal_code = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact zip/postal code: " + contactZipcode);
            } else if (line.startsWith("!Platform_contact_country")) {
                contactCountry = line.replace("!Platform_contact_country = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact country: " + contactCountry);
            } else if (line.startsWith("!Platform_contact_laboratory")) {
                contactLaboratory = line.replace("!Platform_contact_laboratory = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact laboratory: " + contactLaboratory);
            } else if (line.startsWith("!Platform_contact_department")) {
                contactDepartment = line.replace("!Platform_contact_department = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact department: " + contactDepartment);
            } else if (line.startsWith("!Platform_contact_institute")) {
                contactInstitution = line.replace("!Platform_contact_institute = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact institute: " + contactInstitution);
            } else if (line.startsWith("!Platform_contact_web_link")) {
                contactWebLink = line.replace("!Platform_contact_web_link = ", "");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contact web link: " + contactWebLink);
            } else if (line.startsWith("!Platform_contributor")) {
                contributor.addStore(line, "!Platform_contributor = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform contributor: " + contributor.getCurrentStore());
            } else if (line.startsWith("!Platform_description")) {
                description.addStore(line, "!Platform_description = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform description: " + description.getCurrentStore());
            } else if (line.startsWith("!Platform_catalog_number")) {
                catalogNumber.addStore(line, "!Platform_catalog_number = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform catalog number: " + catalogNumber.getCurrentStore());
            } else if (line.startsWith("!Platform_manufacture_protocol")) {
                manufactureProtocol.addStore(line, "!Platform_manufacture_protocol = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform manufacture protocol: " + manufactureProtocol.getCurrentStore());
            } else if (line.startsWith("!Platform_manufacturer")) {
                manufacturer.addStore(line, "!Platform_manufacturer = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform manufacturer: " + manufacturer.getCurrentStore());
            } else if (line.startsWith("!Platform_relation")) {
                relation.addStore(line, "!Platform_relation = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform relation: " + relation.getCurrentStore());
            } else if (line.startsWith("!Platform_supplementary_file")) {
                supplementaryFile.addStore(line, "!Platform_supplementary_file = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform supplementary file: " + supplementaryFile.getCurrentStore());
            } else if (line.startsWith("!Platform_support")) {
                support.addStore(line, "!Platform_support = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform support: " + support.getCurrentStore());
            } else if (line.startsWith("!Platform_web_link")) {
                webLink.addStore(line, "!Platform_web_link = ");
                if (SoftFileParser.DEBUG)
                    System.out.println("Platform web link: " + webLink.getCurrentStore());
            } else {
                if (SoftFileParser.DEBUG) System.out.println(line);
            }
        //}
    }

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

    public String getTechnology() {
        return technology;
    }

    public String getDistribution() {
        return distribution;
    }

    public String getOrganism() {
        return organism;
    }

    public String getTaxID() {
        return taxID;
    }

    public String getDataRowCount() {
        return dataRowCount;
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

    public String getContactLaboratory() {
        return contactLaboratory;
    }

    public String getContactDepartment() {
        return contactDepartment;
    }

    public String getContactInstitution() {
        return contactInstitution;
    }

    public String getContactWebLink() {
        return contactWebLink;
    }

    public Subtype getContributor() {
        return contributor;
    }

    public Subtype getDescription() {
        return description;
    }

    public Subtype getCatalogNumber() {
        return catalogNumber;
    }

    public Subtype getManufactureProtocol() {
        return manufactureProtocol;
    }

    public Subtype getManufacturer() {
        return manufacturer;
    }

    public Subtype getRelation() {
        return relation;
    }

    public Subtype getSupplementaryFile() {
        return supplementaryFile;
    }

    public Subtype getSupport() {
        return support;
    }

    public Subtype getWebLink() {
        return webLink;
    }
}
