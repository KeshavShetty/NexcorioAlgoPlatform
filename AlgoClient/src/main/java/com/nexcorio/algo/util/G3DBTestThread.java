package com.nexcorio.algo.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.util.db.HDataSource;

public class G3DBTestThread implements Runnable {

	private static final Logger log = LogManager.getLogger(G3DBTestThread.class);

	public Long id;
	
	public float premiumSpikePercent = 8f;
	
	public G3DBTestThread(Long id) {
		this.id = id;
		
		Thread t = new Thread(this, "DBTEstThread-"+id);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		
		try {
			// Insert
			dbOperation(1);
			Thread.sleep(10);
			// Fetch
			dbOperation(2);
			Thread.sleep(10);
			// Delete
			dbOperation(3);
			Thread.sleep(10);
			//Fetch again
			dbOperation(2);
			HDataSource.logHikariStats();
		} catch (Exception e) {			
			
			log.error("Error"+e.getMessage(), e);
			
		}
	}
	
	private void dbOperation(int operationType) throws Exception {
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			if (operationType==1) {
				stmt.execute("INSERT INTO simple_file (id, filename, size)"
						+ "VALUES(" + this.id +  ",'Keshav Shetty'," + (120+id) + ")");
			} else if (operationType==2) {
				ResultSet rs = stmt.executeQuery("SELECT * from simple_file where id=" + this.id);
				log.info("FetchSize="+rs.getFetchSize()); 
				rs.close();
			} else if (operationType==3) {
				stmt.execute("DELETE from simple_file where id=" + this.id);
			}
			
			stmt.close();
		} catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
