package net.quasardb.qdb;

import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
// import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

public class Logger
{
    public static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(Logger.class);

    /**
     * Configure
     *
     * @param configFile The file you wish to log into.
     */
    public static void configure(String configFile)
    {
        // this works, but the config seems off you should see the following message at the start of the tests if you
        // call this function:
        //
        // ERROR StatusLogger No Log4j 2 configuration file found.
        // Using default configuration (logging only errors to the console),
        // or user programmatically provided configurations.
        // Set system property 'log4j2.debug' to show Log4j 2 internal initialization logging.
        // See https://logging.apache.org/log4j/2.x/manual/configuration.html for instructions on how to configure Log4j
        // 2
        ThreadContext.put("fileName", configFile);
        logger.error("Here's some info!");
    }
}
