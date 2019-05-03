package com.putti.web3;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

/**
 * Servlet implementation class DatastoreAPI
 */
@WebServlet("/DatastoreAPI")
public class DatastoreAPI extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DatastoreAPI() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String str = request.getParameter("case");
		if (str == null) {
			str = "0";
		}
		
		int c = Integer.valueOf(str);
		response.getWriter().append("DatastoreAPI handling case:" + str + "\n");
		
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
		
	}
	
	private void testReadCase2(HttpServletRequest request, HttpServletResponse response) {
		
	}
	
	private void testReadCase1(HttpServletRequest request, HttpServletResponse response) {
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		KeyFactory keyFactory = new KeyFactory("putti-project");
		keyFactory.setKind("DeviceData");
		Query<Entity> query = Query.newEntityQueryBuilder()
			    .setKind("DeviceData")
			    .setFilter(PropertyFilter.eq("__key__", keyFactory.newKey("083D1BC8CDCA4F57AE94B26D83A7D63A-0002d8a7-5f88-41d6-8cb8-070290bd633f")))
			    .build();
		QueryResults<Entity> results = datastore.run(query);
		while (results.hasNext()) {
		  Entity currentEntity = results.next();
		  System.out.println(currentEntity.getString("name") + ", you're invited to a pizza party!");
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
