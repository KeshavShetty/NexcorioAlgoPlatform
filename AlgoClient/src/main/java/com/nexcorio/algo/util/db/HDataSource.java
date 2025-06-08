package com.nexcorio.algo.util.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.Main;
import com.nexcorio.algo.util.ApplicationConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class HDataSource {
	 
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    
    private static HikariConfig readOnlyConfig = new HikariConfig();
    private static HikariDataSource readOnlyDs;
    
    private static final Logger log = LogManager.getLogger(Main.class);
 
    static {
        config.setJdbcUrl( ApplicationConfig.getProperty("nexcorio.database.url") );
        config.setUsername( ApplicationConfig.getProperty("nexcorio.database.user") );
        config.setPassword( ApplicationConfig.getProperty("nexcorio.database.password") );
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        config.addDataSourceProperty( "socketTimeout" , "120" );
        config.setMaxLifetime(120000 );
        config.setMinimumIdle(5);
        config.setConnectionTimeout(120000 );
        config.setIdleTimeout(60000);
        config.setMaximumPoolSize(75);
        
        config.setLeakDetectionThreshold(120000);
        ds = new HikariDataSource( config );
        
        readOnlyConfig.setJdbcUrl( ApplicationConfig.getProperty("nexcorio.database.url") );
        readOnlyConfig.setUsername( ApplicationConfig.getProperty("nexcorio.database.user") );
        readOnlyConfig.setPassword( ApplicationConfig.getProperty("nexcorio.database.password") );
        readOnlyConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        readOnlyConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        readOnlyConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        readOnlyConfig.addDataSourceProperty( "socketTimeout" , "120" );
        readOnlyConfig.setMaxLifetime(120000 );
        readOnlyConfig.setMinimumIdle(5);
        readOnlyConfig.setConnectionTimeout(120000 );
        readOnlyConfig.setIdleTimeout(60000);
        readOnlyConfig.setMaximumPoolSize(125);
        
        readOnlyConfig.setAutoCommit(false);
        readOnlyConfig.setReadOnly(true); // <- Extra
        
        readOnlyConfig.setLeakDetectionThreshold(120000);
        readOnlyDs = new HikariDataSource( readOnlyConfig );
        
    }
 
    private HDataSource() {}
 
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
    
    public static Connection getReadOnlyConnection() throws SQLException {
        return readOnlyDs.getConnection();
    }
    
    private static void printHikariStats(HikariDataSource dsRef) {
    	HikariPoolMXBean poolMXBean = dsRef.getHikariPoolMXBean();
        int activeConnections = poolMXBean.getActiveConnections();
        int idleConnections = poolMXBean.getIdleConnections();
        int totalConnections = poolMXBean.getTotalConnections();
        int threadsAwaitingConnection = poolMXBean.getThreadsAwaitingConnection();
        String stats = "Total[" + totalConnections + "],Active[" + activeConnections + "],Idle[" + idleConnections + "],Waiting[" + threadsAwaitingConnection + "]";
        System.out.println("=== Hikari Stats=== " + stats);
        log.info("=== Hikari Stats=== " + stats);
    }
    
    public static void logHikariStats() {
    	System.out.println("---------Ds---------");
    	printHikariStats(ds);
    	System.out.println("---------readOnlyDs---------");
    	printHikariStats(readOnlyDs);
    }
}