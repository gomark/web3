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
 * Servlet implementation class DatastoreAPI4
 */
@WebServlet("/DatastoreAPI4")
public class DatastoreAPI4 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DatastoreAPI4() {
        super();
        // TODO Auto-generated constructor stub
    }

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
				
		}
		
		long endMil = System.currentTimeMillis();
		long elapsed = endMil-startMil;
		
		response.getWriter().append("\nelapsed=" + String.valueOf(elapsed) + " mil\n");
	}
	
	private void testReadCase1(HttpServletRequest request, HttpServletResponse response) {
		try {
			long startMil = System.currentTimeMillis();
			Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
			long endMil = System.currentTimeMillis();
			long elapsed = endMil-startMil;
			System.out.println(".Connection elapsed=" + String.valueOf(elapsed));
			response.getWriter().append(".Connection elapsed=" + String.valueOf(elapsed) + "\n");
			
			KeyFactory keyFactory = new KeyFactory("putti-project-4");			
			keyFactory.setKind("Token");
			
			Query<Entity> query = Query.newEntityQueryBuilder()					
				    .setKind("Token")
				    .setFilter(PropertyFilter.eq("__key__", keyFactory.newKey(5634161670881280l)))
				    .build();
			
			startMil = System.currentTimeMillis();
			QueryResults<Entity> results = datastore.run(query);
			endMil = System.currentTimeMillis();
			elapsed = endMil-startMil;
			System.out.println("Query elapsed=" + String.valueOf(elapsed));
			response.getWriter().append("Query elapsed=" + String.valueOf(elapsed) + "\n");
			
			while (results.hasNext()) {
				Entity currentEntity = results.next();
				response.getWriter().append("customer_id=" + currentEntity.getString("customerId") + ", you're invited to a pizza party!\n ");
			}			
		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
			
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
