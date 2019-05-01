package com.putti.web3;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.api.client.util.Base64;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.gson.Gson;



/**
 * Servlet implementation class TestBigTableAPI
 */
@WebServlet("/TestBigTableAPI")
public class TestBigTableAPI extends HttpServlet {
	private final Logger log = Logger.getLogger(TestBigTableAPI.class.getName());
	private static final long serialVersionUID = 1L;
	
	private String projectId = "putti-project2";
	private String instanceId = "putti-bigtable1";
	
	private static final String[] GREETINGS =
	      { "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!" };
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TestBigTableAPI() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    private void readSingleRowFromKey(HttpServletRequest request, HttpServletResponse response) {
    	byte[] tableName = Bytes.toBytes("my-table");
    	byte[] columeFamily = Bytes.toBytes("cf1");
    	byte[] columnName = Bytes.toBytes("greeting");
    	
    	try {
    		Connection conn = BigtableConfiguration.connect(this.projectId, this.instanceId);
    		Table table = conn.getTable(TableName.valueOf(tableName));
        	String rowKey = "greeting1";
            Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
            if (getResult.isEmpty() == false) {
            	            	
            	String greeting = Bytes.toString(getResult.getValue(columeFamily, columnName));
            	response.getWriter().append("\nGreeting={" + greeting + "}");

            } else {
            	response.getWriter().append("\nNot found rowKey=" + rowKey);
            }
            
            
            table.close();
            log.info("readSingleRowFromKey()");
                		
            conn.close();
    	} catch (Exception e) {
    		log.log(Level.SEVERE, e.toString());
    	}
    	    	
    }     

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		/*
		response.getWriter().append("Why? Served at: ").append(request.getContextPath());
		this.readSingleRowFromKey(request, response);
		*/
		String str = request.getParameter("case");
		if (str == null) {
			str = "0";
		}
		
		int c = Integer.valueOf(str);
		response.getWriter().append("handling case:" + str + "\n");
		
		long startMil = System.currentTimeMillis();
		
		switch (c) {
			case 0:
				response.getWriter().append("missing querystring 'case' \n");
				break;
				
			case 1:	// Reading single record
				this.testReadCase1(request, response);
				break;
				
			case 2: // Reading multiple record
				this.testReadCase2(request, response);
				break;
				
			case 3: // Update single record
				this.testReadCase3(request, response);
				break;
				
		}
		
		long endMil = System.currentTimeMillis();
		long elapsed = endMil-startMil;
		
		response.getWriter().append("\nelapsed=" + String.valueOf(elapsed) + " mil\n");
		
	}
	
	private void testReadCase3(HttpServletRequest request, HttpServletResponse response) {
		byte[] tableName = Bytes.toBytes("tbl-stock");
		byte[] columnFamily = Bytes.toBytes("csv");
		byte[] columnName = Bytes.toBytes("Close");
		
		Connection conn = null;
		log.info("Update single records");
		
    	try {
    		conn = BigtableConfiguration.connect(this.projectId, this.instanceId);
    		
    		Table table = conn.getTable(TableName.valueOf(tableName));
        	String rowKey = "MMM#2006-01-11";

        	Put p1 = new Put(Bytes.toBytes(rowKey));
        	p1.add(columnFamily, columnName, Bytes.toBytes("900"));
        	table.put(p1);
                            		
    	} catch (Exception e) {
    		log.log(Level.SEVERE, e.toString());
    	    		
    	}	
    	
    	if (conn != null) {
    		try {
    			if (conn.isClosed() == false) conn.close();	
    		} catch (Exception e) {
    			log.log(Level.SEVERE, e.toString());
    		}
    		
    	}		
	}	
	
	private void testReadCase2(HttpServletRequest request, HttpServletResponse response) {
		byte[] tableName = Bytes.toBytes("tbl-stock");
		byte[] columeFamily = Bytes.toBytes("csv");
		byte[] columnName = Bytes.toBytes("Close");
			
		Connection conn = null;
		log.info("Reading multiple records..");
		
		Scan scan = new Scan();
		FilterList fl = new FilterList();
		
    	try {
    		//conn = BigtableConfiguration.connect(this.projectId, this.instanceId);
    		conn = (Connection) this.getServletContext().getAttribute("bt_conn");
    		Table table = conn.getTable(TableName.valueOf(tableName));
        	
    		/*
    		String rowKey = "MMM#2006-01-11";        	
    		RowFilter rf = new RowFilter(CompareOp.EQUAL, new BinaryComparator(rowKey.getBytes()));        
        	fl.addFilter(rf);
			*/
    		
    		String prefixStr = "MMM";
    		response.getWriter().append("using rowKey prefix=" + prefixStr + "\n");
    		byte[] bPrefix = Bytes.toBytes(prefixStr);
    		Filter prefixFilter = new PrefixFilter(bPrefix);
    		fl.addFilter(prefixFilter);
    		
        	scan.setFilter(fl);
        	ResultScanner resultScanner = table.getScanner(scan);
        	for (Result result : resultScanner) {
        		String closeValue = Bytes.toString(result.getValue(columeFamily, columnName));
        		String rowKey = Bytes.toString(result.getRow());
        		response.getWriter().append("by-prefix:" + rowKey + ", Close=" + closeValue + ", timestamp=" + String.valueOf(result.rawCells()[0].getTimestamp()) + "\n");
        	}
            table.close();
            
                		
    	} catch (Exception e) {
    		log.log(Level.SEVERE, e.toString());
    		e.printStackTrace();
    	    		
    	}	
		
	}
	
	private void testReadCase1(HttpServletRequest request, HttpServletResponse response) {
		byte[] tableName = Bytes.toBytes("tbl-stock");
		byte[] columeFamily = Bytes.toBytes("csv");
		byte[] columnName = Bytes.toBytes("Close");
		
		Connection conn = null;
		log.info("Reading single record..");
		
    	try {
    		conn = BigtableConfiguration.connect(this.projectId, this.instanceId);
    		Table table = conn.getTable(TableName.valueOf(tableName));
        	String rowKey = "MMM#2006-01-11";
            Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
            if (getResult.isEmpty() == false) {
            	            	
            	String closeValue = Bytes.toString(getResult.getValue(columeFamily, columnName));
            	response.getWriter().append("\nClose={" + closeValue + "}\n");

            } else {
            	response.getWriter().append("\nNot found rowKey=" + rowKey);
            }
                        
            table.close();
            
                		
    	} catch (Exception e) {
    		log.log(Level.SEVERE, e.toString());
    	    		
    	}	
    	
    	if (conn != null) {
    		try {
    			if (conn.isClosed() == false) conn.close();	
    		} catch (Exception e) {
    			log.log(Level.SEVERE, e.toString());
    		}
    		
    	}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
