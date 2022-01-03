package com.arcadedb;

import com.arcadedb.log.Logger;

import java.util.logging.Level;

public enum NullLogger implements Logger {

    INSTANCE;

    @Override
    public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object arg1, Object arg2,
                    Object arg3, Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, Object arg10, Object arg11,
                    Object arg12, Object arg13, Object arg14, Object arg15, Object arg16, Object arg17) {
    }

    @Override
    public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object... args) {
    }

    @Override
    public void flush() {

    }

}
