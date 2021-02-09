package com.baichen.jraft.meta;


public interface StateStorageFactory {

    StateStorage create(MetaStorageOptions options);

    String getType();
}
