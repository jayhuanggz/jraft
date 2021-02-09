package com.baichen.jraft.meta;

import java.util.Properties;

public class MetaStorageOptions {

    private String type = "rocksdb";

    private String dbFile;

    private Properties options = new Properties();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Properties getOptions() {
        return options;
    }

    public void setOptions(Properties options) {
        this.options = options;
    }

    public String getDbFile() {
        return dbFile;
    }

    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }
}
