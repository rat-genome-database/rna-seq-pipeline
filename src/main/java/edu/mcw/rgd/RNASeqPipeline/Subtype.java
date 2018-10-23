package edu.mcw.rgd.RNASeqPipeline;

import java.util.ArrayList;
import java.util.List;

class Subtype {
    List<String> store = new ArrayList<String>(0);

    void addStore(String line, String target) {
        this.store.add(line.replace(target, ""));
    }

    int getStoreLength() {
        return this.store.size();
    }

    String writeStoreLength() {
        return String.valueOf(getStoreLength());
    }

    int setStoreIndex() {
        int index = getStoreLength() - 1;

        if (index < 0) return 0;
        else return index;
    }

    String getStore(int index) {
        return this.store.get(index);
    }

    String getCurrentStore() {
        return this.getStore(this.setStoreIndex());
    }

    String writeStore() {
        if (this.getStoreLength() == 0) return "";

        String text = "";

        for (int i = 0; i < this.getStoreLength(); i++) {
            if (i == this.getStoreLength() - 1) text += this.getStore(i);
            else text += this.getStore(i) + "||";
        }

        return text;
    }

    String getStoreStr() {
        if (this.getStoreLength() == 0) return "";

        String text = "";

        for (int i = 0; i < this.getStoreLength(); i++) {
            if (i == this.getStoreLength() - 1) text += this.getStore(i);
            else text += this.getStore(i) + "||";
        }

        return text;
    }

}
