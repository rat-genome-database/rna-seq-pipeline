package edu.mcw.rgd.RNASeqPipeline;

import org.apache.logging.log4j.Logger;

import java.util.List;

public class Sample {
    private String channelCount, dataRowCount, title, description,
            geoAccessionID, instrumentModel, status, type,
            submissionDate, lastUpdateDate, seriesID,
            contactAddress, contactCity, contactCountry,
            contactDepartment, contactEmail, contactFax,
            contactInstitution, contactLaboratory, contactName,
            contactPhone, contactState, contactWebLink, contactZipcode,
            label_ch1, label_ch2,
            labelProtocol_ch1, labelProtocol_ch2,
            librarySelection, librarySource, libraryStrategy,
            molecule_ch1, molecule_ch2,
            organism_ch1, organism_ch2,
            platformID, taxID_ch1, taxID_ch2,
            sourceName_ch1, sourceName_ch2,
            treatmentProtocol_ch1, treatmentProtocol_ch2;

    private Subtype dataProcessing = new Subtype();
    private Subtype relation = new Subtype();
    private Subtype characteristics_ch1 = new Subtype();
    private Subtype characteristics_ch2 = new Subtype();
    private Subtype extractProtocol_ch1 = new Subtype();
    private Subtype extractProtocol_ch2 = new Subtype();
    private Subtype growthProtocol_ch1 = new Subtype();
    private Subtype growthProtocol_ch2 = new Subtype();

    private Subtype supplementaryFile = new Subtype();


    public void handleSample(String line, Logger log) {
        /*if (line.startsWith("^SAMPLE")) {
            addSample();
            if (SoftFileParser.DEBUG) System.out.println("Sample: " + line.replace("^SAMPLE = ", ""));
        } else {*/
        // String variables
        if (line.startsWith("!Sample_channel_count")) {
            channelCount = line.replace("!Sample_channel_count = ", "");
            log.debug("Sample channel count: " + channelCount);
        } else if (line.startsWith("!Sample_data_row_count")) {
            dataRowCount = line.replace("!Sample_data_row_count = ", "");
            log.debug("Sample data row count: " + dataRowCount);
        } else if (line.startsWith("!Sample_title")) {
            title = line.replace("!Sample_title = ", "");
            log.debug("Sample title: " + title);
        } else if (line.startsWith("!Sample_description")) {
            description = line.replace("!Sample_description = ", "");
            log.debug("Sample description: " + description);
        } else if (line.startsWith("!Sample_geo_accession")) {
            geoAccessionID = line.replace("!Sample_geo_accession = ", "");
            log.debug("Sample geo accession: " + geoAccessionID);
        } else if (line.startsWith("!Sample_instrument_model")) {
            instrumentModel = line.replace("!Sample_instrument_model = ", "");
            log.debug("Sample instrument model: " + instrumentModel);
        } else if (line.startsWith("!Sample_status")) {
            status = line.replace("!Sample_status = ", "");
            log.debug("Sample status: " + status);
        } else if (line.startsWith("!Sample_type")) {
            type = line.replace("!Sample_type = ", "");
            log.debug("Sample type: " + type);
        } else if (line.startsWith("!Sample_submission_date")) {
            submissionDate = line.replace("!Sample_submission_date = ", "");
            log.debug("Sample submission date: " + submissionDate);
        } else if (line.startsWith("!Sample_last_update_date")) {
            lastUpdateDate = line.replace("!Sample_last_update_date = ", "");
            log.debug("Sample last update date: " + lastUpdateDate);
        } else if (line.startsWith("!Sample_series_id")) {
            seriesID = line.replace("!Sample_series_id = ", "");
            log.debug("Sample series id: " + seriesID);
        } else if (line.startsWith("!Sample_contact_address")) {
            contactAddress = line.replace("!Sample_contact_address = ", "");
            log.debug("Sample contact address: " + contactAddress);
        } else if (line.startsWith("!Sample_contact_city")) {
            contactCity = line.replace("!Sample_contact_city = ", "");
            log.debug("Sample contact city: " + contactCity);
        } else if (line.startsWith("!Sample_contact_country")) {
            contactCountry = line.replace("!Sample_contact_country = ", "");
            log.debug("Sample contact country: " + contactCountry);
        } else if (line.startsWith("!Sample_contact_department")) {
            contactDepartment = line.replace("!Sample_contact_department = ", "");
            log.debug("Sample contact department: " + contactDepartment);
        } else if (line.startsWith("!Sample_contact_email")) {
            contactEmail = line.replace("!Sample_contact_email = ", "");
            log.debug("Sample contact email: " + contactEmail);
        } else if (line.startsWith("!Sample_contact_fax")) {
            contactFax = line.replace("!Sample_contact_fax = ", "");
            log.debug("Sample contact fax: " + contactFax);
        } else if (line.startsWith("!Sample_contact_institute")) {
            contactInstitution = line.replace("!Sample_contact_institute = ", "");
            log.debug("Sample contact institute: " + contactInstitution);
        } else if (line.startsWith("!Sample_contact_laboratory")) {
            contactLaboratory = line.replace("!Sample_contact_laboratory = ", "");
            log.debug("Sample contact laboratory: " + contactLaboratory);
        } else if (line.startsWith("!Sample_contact_name")) {
            contactName = line.replace("!Sample_contact_name = ", "");
            log.debug("Sample contact name: " + contactName);
        } else if (line.startsWith("!Sample_contact_phone")) {
            contactPhone = line.replace("!Sample_contact_phone = ", "");
            log.debug("Sample contact phone: " + contactPhone);
        } else if (line.startsWith("!Sample_contact_state")) {
            contactState = line.replace("!Sample_contact_state = ", "");
            log.debug("Sample contact state: " + contactState);
        } else if (line.startsWith("!Sample_contact_web_link")) {
            contactWebLink = line.replace("!Sample_contact_web_link = ", "");
            log.debug("Sample contact web link: " + contactWebLink);
        } else if (line.startsWith("!Sample_contact_zip/postal_code")) {
            contactZipcode = line.replace("!Sample_contact_zip/postal_code = ", "");
            log.debug("Sample contact zip/postal code: " + contactZipcode);
        } else if (line.startsWith("!Sample_label_ch1")) {
            label_ch1 = line.replace("!Sample_label_ch1 = ", "");
            log.debug("Sample label ch1: " + label_ch1);
        } else if (line.startsWith("!Sample_label_ch2")) {
            label_ch2 = line.replace("!Sample_label_ch2 = ", "");
            log.debug("Sample label ch2: " + label_ch2);
        } else if (line.startsWith("!Sample_label_protocol_ch1")) {
            labelProtocol_ch1 = line.replace("!Sample_label_protocol_ch1 = ", "");
            log.debug("Sample label protocol ch1: " + labelProtocol_ch1);
        } else if (line.startsWith("!Sample_label_protocol_ch2")) {
            labelProtocol_ch2 = line.replace("!Sample_label_protocol_ch2 = ", "");
            log.debug("Sample label protocol ch2: " + labelProtocol_ch2);
        } else if (line.startsWith("!Sample_library_selection")) {
            librarySelection = line.replace("!Sample_library_selection = ", "");
            log.debug("Sample library selection: " + librarySelection);
        } else if (line.startsWith("!Sample_library_source")) {
            librarySource = line.replace("!Sample_library_source = ", "");
            log.debug("Sample library source: " + librarySource);
        } else if (line.startsWith("!Sample_library_strategy")) {
            libraryStrategy = line.replace("!Sample_library_strategy = ", "");
            log.debug("Sample library strategy: " + libraryStrategy);
        } else if (line.startsWith("!Sample_molecule_ch1")) {
            molecule_ch1 = line.replace("!Sample_molecule_ch1 = ", "");
            log.debug("Sample molecule ch1: " + molecule_ch1);
        } else if (line.startsWith("!Sample_molecule_ch2")) {
            molecule_ch2 = line.replace("!Sample_molecule_ch2 = ", "");
            log.debug("Sample molecule ch2: " + molecule_ch2);
        } else if (line.startsWith("!Sample_organism_ch1")) {
            organism_ch1 = line.replace("!Sample_organism_ch1 = ", "");
            log.debug("Sample organism ch1: " + organism_ch1);
        } else if (line.startsWith("!Sample_organism_ch2")) {
            organism_ch2 = line.replace("!Sample_organism_ch2 = ", "");
            log.debug("Sample organism ch2: " + organism_ch2);
        } else if (line.startsWith("!Sample_platform_id")) {
            platformID = line.replace("!Sample_platform_id = ", "");
            log.debug("Sample platform id: " + platformID);
        } else if (line.startsWith("!Sample_taxid_ch1")) {
            taxID_ch1 = line.replace("!Sample_taxid_ch1 = ", "");
            log.debug("Sample taxid ch1: " + taxID_ch1);
        } else if (line.startsWith("!Sample_taxid_ch2")) {
            taxID_ch2 = line.replace("!Sample_taxid_ch2 = ", "");
            log.debug("Sample taxid ch2: " + taxID_ch2);
        } else if (line.startsWith("!Sample_source_name_ch1")) {
            sourceName_ch1 = line.replace("!Sample_source_name_ch1 = ", "");
            log.debug("Sample source name ch1: " + sourceName_ch1);
        } else if (line.startsWith("!Sample_source_name_ch2")) {
            sourceName_ch2 = line.replace("!Sample_source_name_ch2 = ", "");
            log.debug("Sample source name ch2: " + sourceName_ch2);
        } else if (line.startsWith("!Sample_treatment_protocol_ch1")) {
            treatmentProtocol_ch1 = line.replace("!Sample_treatment_protocol_ch1 = ", "");
            log.debug("Sample treatment protocol ch1: " + treatmentProtocol_ch1);
        } else if (line.startsWith("!Sample_treatment_protocol_ch2")) {
            treatmentProtocol_ch2 = line.replace("!Sample_treatment_protocol_ch2 = ", "");
            log.debug("Sample treatment protocol ch2: " + treatmentProtocol_ch2);
        }

        // Handling of Subtypes
        else if (line.startsWith("!Sample_data_processing")) {
            dataProcessing.addStore(line, "!Sample_data_processing = ");
            log.debug("Sample data processing: " + dataProcessing.getCurrentStore());
        } else if (line.startsWith("!Sample_relation")) {
            relation.addStore(line, "!Sample_relation = ");
            log.debug("Sample relation: " + relation.getCurrentStore());
        } else if (line.startsWith("!Sample_characteristics_ch1")) {
            characteristics_ch1.addStore(line, "!Sample_characteristics_ch1 = ");
            log.debug("Sample characteristics ch1: " + characteristics_ch1.getCurrentStore());
        } else if (line.startsWith("!Sample_characteristics_ch2")) {
            characteristics_ch2.addStore(line, "!Sample_characteristics_ch2 = ");
            log.debug("Sample characteristics ch2: " + characteristics_ch2.getCurrentStore());
        } else if (line.startsWith("!Sample_extract_protocol_ch1")) {
            extractProtocol_ch1.addStore(line, "!Sample_extract_protocol_ch1 = ");
            log.debug("Sample extract protocol ch1: " + extractProtocol_ch1.getCurrentStore());
        } else if (line.startsWith("!Sample_extract_protocol_ch2")) {
            extractProtocol_ch2.addStore(line, "!Sample_extract_protocol_ch2 = ");
            log.debug("Sample extract protocol ch2: " + extractProtocol_ch2.getCurrentStore());
        } else if (line.startsWith("!Sample_growth_protocol_ch1")) {
            growthProtocol_ch1.addStore(line, "!Sample_growth_protocol_ch1 = ");
            log.debug("Sample growth protocol ch1: " + growthProtocol_ch1.getCurrentStore());
        } else if (line.startsWith("!Sample_growth_protocol_ch2")) {
            growthProtocol_ch2.addStore(line, "!Sample_growth_protocol_ch2 = ");
            log.debug("Sample growth protocol ch2: " + growthProtocol_ch2.getCurrentStore());
        }

        // Special case: regular expression handling of supplementary files
        else if (line.startsWith("!Sample_supplementary_file")) {
            String trueLine = line.replaceFirst("^!Sample_supplementary_file[_1-9]* = ", "");
            supplementaryFile.store.add(trueLine);
            log.debug("Sample supplementary file: " + supplementaryFile.getCurrentStore());
        } else {
            log.debug(line);
        }
        // }
    }

    String getStrain() {
        List<String> store = this.characteristics_ch1.store;

        for (int i = 0; i < store.size(); i++) {
            if (store.get(i).startsWith("strain")) return store.get(i).replaceFirst("^strain.*: ", "");
        }

        return null;
    }

    public String getAge() {
        List<String> store = this.characteristics_ch1.store;

        for (int i = 0; i < store.size(); i++) {
            if (store.get(i).startsWith("age (weeks):")) return store.get(i).replace("age (weeks): ", "") + " weeks";
            else if (store.get(i).startsWith("age")) return store.get(i).replaceFirst("^age.*: ", "");
            else if (store.get(i).startsWith("developmental stage (week):"))
                return store.get(i).replace("developmental stage (week): ", "") + " weeks";
            else if (store.get(i).startsWith("developmental stage:"))
                return store.get(i).replace("developmental stage: ", "");
            else if (store.get(i).startsWith("time:")) return store.get(i).replace("time: ", "");
        }

        
        return null;
    }

    public String getGender() {
        List<String> characteristics = this.characteristics_ch1.store;
        List<String> growthProtocol = this.growthProtocol_ch1.store;

        for (int i = 0; i < characteristics.size(); i++) {
            if (characteristics.get(i).startsWith("gender:"))
                return characteristics.get(i).replace("gender: ", "");
            else if (characteristics.get(i).startsWith("Sex:"))
                return characteristics.get(i).replace("Sex: ", "");
        }

        for (int i = 0; i < growthProtocol.size(); i++) {
            if (growthProtocol.get(i).matches(".*(?i)female.*")) return "Female";
            else if (growthProtocol.get(i).matches(".*(?i)male.*")) return "Male";
        }

        return null;
    }

    public String getTissue() {
        List<String> store = this.characteristics_ch1.store;

        for (int i = 0; i < store.size(); i++) {
            if (store.get(i).startsWith("tissue:")) return store.get(i).replace("tissue: ", "");
        }

        return this.sourceName_ch1;
    }

    String getCellType() {
        List<String> store = this.characteristics_ch1.store;

        for (int i = 0; i < store.size(); i++) {
            if (store.get(i).startsWith("cell type:")) return store.get(i).replace("cell type: ", "");
        }

        return null;
    }

    String getCellLine() {
        List<String> store = this.characteristics_ch1.store;

        for (int i = 0; i < store.size(); i++) {
            if (store.get(i).matches("^cell.line.*")) return store.get(i).replaceFirst("^cell.line: ", "");
        }

        return null;
    }

    public String getChannelCount() {
        return channelCount;
    }

    public String getDataRowCount() {
        return dataRowCount;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public String getGeoAccessionID() {
        return geoAccessionID;
    }

    public String getInstrumentModel() {
        return instrumentModel;
    }

    public String getStatus() {
        return status;
    }

    public String getType() {
        return type;
    }

    public String getSubmissionDate() {
        return submissionDate;
    }

    public String getLastUpdateDate() {
        return lastUpdateDate;
    }

    public String getSeriesID() {
        return seriesID;
    }

    public String getContactAddress() {
        return contactAddress;
    }

    public String getContactCity() {
        return contactCity;
    }

    public String getContactCountry() {
        return contactCountry;
    }

    public String getContactDepartment() {
        return contactDepartment;
    }

    public String getContactEmail() {
        return contactEmail;
    }

    public String getContactFax() {
        return contactFax;
    }

    public String getContactInstitution() {
        return contactInstitution;
    }

    public String getContactLaboratory() {
        return contactLaboratory;
    }

    public String getContactName() {
        return contactName;
    }

    public String getContactPhone() {
        return contactPhone;
    }

    public String getContactState() {
        return contactState;
    }

    public String getContactWebLink() {
        return contactWebLink;
    }

    public String getContactZipcode() {
        return contactZipcode;
    }

    public String getLabel_ch1() {
        return label_ch1;
    }

    public String getLabel_ch2() {
        return label_ch2;
    }

    public String getLabelProtocol_ch1() {
        return labelProtocol_ch1;
    }

    public String getLabelProtocol_ch2() {
        return labelProtocol_ch2;
    }

    public String getLibrarySelection() {
        return librarySelection;
    }

    public String getLibrarySource() {
        return librarySource;
    }

    public String getLibraryStrategy() {
        return libraryStrategy;
    }

    public String getMolecule_ch1() {
        return molecule_ch1;
    }

    public String getMolecule_ch2() {
        return molecule_ch2;
    }

    public String getOrganism_ch1() {
        return organism_ch1;
    }

    public String getOrganism_ch2() {
        return organism_ch2;
    }

    public String getPlatformID() {
        return platformID;
    }

    public String getTaxID_ch1() {
        return taxID_ch1;
    }

    public String getTaxID_ch2() {
        return taxID_ch2;
    }

    public String getSourceName_ch1() {
        return sourceName_ch1;
    }

    public String getSourceName_ch2() {
        return sourceName_ch2;
    }

    public String getTreatmentProtocol_ch1() {
        return treatmentProtocol_ch1;
    }

    public String getTreatmentProtocol_ch2() {
        return treatmentProtocol_ch2;
    }

    public Subtype getDataProcessing() {
        return dataProcessing;
    }

    public Subtype getRelation() {
        return relation;
    }

    public Subtype getCharacteristics_ch1() {
        return characteristics_ch1;
    }

    public Subtype getCharacteristics_ch2() {
        return characteristics_ch2;
    }

    public Subtype getExtractProtocol_ch1() {
        return extractProtocol_ch1;
    }

    public Subtype getExtractProtocol_ch2() {
        return extractProtocol_ch2;
    }

    public Subtype getGrowthProtocol_ch1() {
        return growthProtocol_ch1;
    }

    public Subtype getGrowthProtocol_ch2() {
        return growthProtocol_ch2;
    }

    public Subtype getSupplementaryFile() {
        return supplementaryFile;
    }
}
