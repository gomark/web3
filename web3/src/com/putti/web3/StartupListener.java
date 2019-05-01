package com.putti.web3;

import java.util.logging.Level;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

public class StartupListener implements ServletContextListener {
	private String projectId = "putti-project2";
	private String instanceId = "putti-bigtable1";
	private Connection conn;
	
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		System.out.println("contextInitialzied");
		System.out.println("Connecting BigTable");
		this.conn = BigtableConfiguration.connect(this.projectId, this.instanceId);
		System.out.println("connected BigTable");
		
		sce.getServletContext().setAttribute("bt_conn", this.conn);
		
		//this.doPubSub();
	}
	
	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		System.out.println("contextDestroyed");
    	if (this.conn != null) {
    		try {
    			System.out.println("Disconnecting Bigtable");
    			if (this.conn.isClosed() == false) conn.close();
    			
    			System.out.println("Disconnected successfully");
    		} catch (Exception e) {
    			System.out.println(e.toString());
    		}
    		
    	}	
	}
	
	
}
