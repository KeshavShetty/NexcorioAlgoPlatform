package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.Main;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class MultiDataSource {
	 
    private static HikariConfig rtxconfig = new HikariConfig();
    private static HikariDataSource rtxds;
    
    private static HikariConfig terraceconfig = new HikariConfig();
    private static HikariDataSource terraceds;
    
    private static final Logger log = LogManager.getLogger(Main.class);
 
    static {
        rtxconfig.setJdbcUrl( "jdbc:postgresql://192.168.0.103:5432/stockPortal_database?gssencmode=disable" );
        rtxconfig.setUsername( "postgres" );
        rtxconfig.setPassword( "jijikos" );
        rtxconfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        rtxconfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        rtxconfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        rtxconfig.addDataSourceProperty( "socketTimeout" , "120" );
        rtxconfig.setMaxLifetime(120000 );
        rtxconfig.setMinimumIdle(5);
        rtxconfig.setConnectionTimeout(120000 );
        rtxconfig.setIdleTimeout(60000);
        rtxconfig.setMaximumPoolSize(50);
        
        rtxconfig.setLeakDetectionThreshold(120000);
        rtxds = new HikariDataSource( rtxconfig );
        
        
        terraceconfig.setJdbcUrl( "jdbc:postgresql://localhost:5432/nexcorio_db?gssencmode=disable" );
        terraceconfig.setUsername( "postgres" );
        terraceconfig.setPassword( "jijikos" );
        terraceconfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        terraceconfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        terraceconfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        terraceconfig.addDataSourceProperty( "socketTimeout" , "120" );
        terraceconfig.setMaxLifetime(120000 );
        terraceconfig.setMinimumIdle(5);
        terraceconfig.setConnectionTimeout(120000 );
        terraceconfig.setIdleTimeout(60000);
        terraceconfig.setMaximumPoolSize(50);
        
        terraceconfig.setLeakDetectionThreshold(120000);
        terraceds = new HikariDataSource( terraceconfig );
    }
 
    private MultiDataSource() {}
 
    public static Connection getRtxConnection() throws SQLException {
        return rtxds.getConnection();
    }
    
    public static Connection getTerraceConnection() throws SQLException {
        return terraceds.getConnection();
    }
    
}