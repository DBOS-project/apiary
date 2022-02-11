package org.dbos.apiary.voltdb;

import org.voltdb.VoltProcedure;

public class VoltApiaryProcedure extends VoltProcedure {

    protected final VoltFunctionInterface i = new VoltFunctionInterface(this);
}
