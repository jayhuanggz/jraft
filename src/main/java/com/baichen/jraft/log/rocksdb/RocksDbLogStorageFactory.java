package com.baichen.jraft.log.rocksdb;

import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.factory.LogStorageFactory;
import com.baichen.jraft.log.options.LogStorageOptions;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;

import java.util.Properties;

public class RocksDbLogStorageFactory implements LogStorageFactory {
    @Override
    public LogStorage create(LogStorageOptions options) {

        Properties properties = options.getOptions();
        String dbFile = options.getDbFile();

        assert dbFile != null;


        DBOptions dbOptions = properties.isEmpty() ? null : DBOptions.getDBOptionsFromProps(new ConfigOptions(), properties);

        if (dbOptions == null) {
            dbOptions = new DBOptions();
        }
        Options rocksdbOptions = new Options(dbOptions, new ColumnFamilyOptions());

        return new RocksDbLogStorage(rocksdbOptions, dbFile);
    }


    @Override
    public String getType() {
        return "rocksdb";
    }
}
