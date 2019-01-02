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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;



/**
 * Servlet implementation class TestBigTableAPI
 */
@WebServlet("/TestBigTableAPI")
public class TestBigTableAPI extends HttpServlet {
	private final Logger log = Logger.getLogger(TestBigTableAPI.class.getName());
	private static final long serialVersionUID = 1L;
	
	private String projectId = "putti-project2";
	private String instanceId = "putti-bigtable1";
	private byte[] tableName = Bytes.toBytes("my-table");
	private byte[] columeFamily = Bytes.toBytes("cf1");
	private byte[] columnName = Bytes.toBytes("greeting");
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
    	try {
    		Connection conn = BigtableConfiguration.connect(this.projectId, this.instanceId);
    		Table table = conn.getTable(TableName.valueOf(this.tableName));
        	String rowKey = "greeting1";
            Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
            if (getResult.isEmpty() == false) {
            	
            	
            	String greeting = Bytes.toString(getResult.getValue(this.columeFamily, this.columnName));
            	response.getWriter().append("\nGreeting={" + greeting + "}");

            } else {
            	response.getWriter().append("\nNot found rowKey=" + rowKey);
            }
                		
    	} catch (Exception e) {
    		log.log(Level.SEVERE, e.toString());
    	}
    	    	
    }     

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Why? Served at: ").append(request.getContextPath());
		this.readSingleRowFromKey(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
