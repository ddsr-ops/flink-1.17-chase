package com.ddsr.state.limit;

/**
 * @author ddsr, created it at 2024/12/25 16:51
 */
@SuppressWarnings("unused")
public class KryoNotForSchemaEvolution {
    /*

    Kryo cannot be used for schema evolution.

    When Kryo is used, there is no possibility for the framework to verify if any incompatible changes have been made.

    This means that if a data-structure containing a given type is serialized via Kryo, then that contained type can not
    undergo schema evolution.

    For example, if a POJO contains a List<SomeOtherPojo>, then the List and its contents are serialized via Kryo and
    schema evolution is not supported for SomeOtherPojo.
     */
}
