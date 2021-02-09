package com.baichen.jraft.meta.rocksdb;

import com.baichen.jraft.meta.MetaStorageOptions;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.meta.StateStorageFactory;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;

import java.util.Properties;

public class RocksDbStateStorageFactory implements StateStorageFactory {
    @Override
    public StateStorage create(MetaStorageOptions options) {

        Properties properties = options.getOptions();
        String dbFile = options.getDbFile();

        assert dbFile != null;

        DBOptions dbOptions = properties.isEmpty() ? null : DBOptions.getDBOptionsFromProps(properties);

        if (dbOptions == null) {
            dbOptions = new DBOptions();
        }
        Options rocksdbOptions = new Options(dbOptions, new ColumnFamilyOptions());
        return new RocksDbStateStorage(rocksdbOptions, dbFile);
    }

    @Override
    public String getType() {
        return "rocksdb";
    }
}
