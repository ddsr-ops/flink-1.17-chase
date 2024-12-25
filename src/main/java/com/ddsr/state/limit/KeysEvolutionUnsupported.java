package com.ddsr.state.limit;

/**
 * @author ddsr, created it at 2024/12/25 16:49
 */
@SuppressWarnings("unused")
public class KeysEvolutionUnsupported {
    /*
    The structure of a key cannot be migrated as this may lead to non-deterministic behavior. For example, if a POJO
    is used as a key and one field is dropped then there may suddenly be multiple separate keys that are now
    identical. Flink has no way to merge the corresponding values.

    Additionally, the RocksDB state backend relies on binary object identity, rather than the hashCode method. Any change
     to the keys’ object structure can lead to non-deterministic behavior.
     */
}
