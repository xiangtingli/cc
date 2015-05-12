package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.lang.Integer;
import io.undertow.io.Sender;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.io.IoCallback;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

// hbase api import
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

// dynamodb api import
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.codehaus.jackson.map.ObjectMapper;
import java.util.Deque;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Comparator;
import java.util.List; 

// connection pool
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MiniSite {

	// sql parameters
	private static HikariDataSource m_hds;
    private static final String m_url = "mysqltest.c9tc8vxvvk40.us-east-1.rds.amazonaws.com";
	private static final String m_port = "3306";
	private static final String m_db = "db_test";
	private static final String m_user = "junyuc";
	private static final String m_password = "password";
	private static final String m_queryPrefix = "SELECT password, name FROM my_tbl WHERE userid=";
	
	// hbase parameters
	private static HTablePool m_htp;
	private static final String m_master = "ec2-54-85-205-212.compute-1.amazonaws.com";
	private static final String m_table = "tbl";
	
	// dynamo parameters
	private static AmazonDynamoDBClient m_ddb;
	private static final String m_dbtbl = "ddbtbl";	
	private static final String m_credp = "/home/ulysses/15619/Project3_3/AwsCredentials.properties";
	static class ResHolder
	{
		public String dat;
		public String url;
		public ResHolder(String _dat, String _url)
		{
			dat = _dat;
			url = _url;
		}
	}

	// all together
	static class AllHolder implements Comparable<AllHolder>
	{
		public String dat;
		public String url;
		public String nam;
		public AllHolder(String _dat, String _url, String _nam)
		{
			dat = _dat;
			url = _url;
			nam = _nam;
		}
		public int compareTo(AllHolder o)
		{
			if(dat.equals(o.dat))
			{
				return nam.compareTo(o.nam);
			}
			return dat.compareTo(o.dat);
		}
	}

    public MiniSite() throws Exception
    {
    	initRDS();
    	initHBase();
    	initDynamo();
    }
	
	private void initRDS() throws Exception
	{
    	HikariConfig config = new HikariConfig();
		config.setMaximumPoolSize(100);
		config.setDataSourceClassName(com.mysql.jdbc.jdbc2.optional.MysqlDataSource.class.getName());
		config.addDataSourceProperty("serverName", m_url);
		config.addDataSourceProperty("port", m_port);
		config.addDataSourceProperty("databaseName", m_db);
		config.addDataSourceProperty("user", m_user);
		config.addDataSourceProperty("password", m_password);
		m_hds = new HikariDataSource(config);
	}
	
	private void initHBase() throws Exception
	{
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", m_master);
        m_htp = new HTablePool(config, 100);
	}
	
	private void initDynamo() throws Exception
	{
		AWSCredentials cred = new PropertiesCredentials(new File("../AwsCredentials.properties"));
		m_ddb = new AmazonDynamoDBClient(cred);
	}
	
	private static String LookupSQL(String usr, String pwd, boolean needAuth)
	{
		Connection conn = null;
		Statement stmt = null; 
		String name = "Unauthorized";
		try
		{
			conn = m_hds.getConnection();
			if(conn != null)
			{
				stmt = conn.createStatement();
				String sql;
				sql = m_queryPrefix + usr;
				ResultSet rs = stmt.executeQuery(sql);
			
				while(rs.next())
				{
					String retPwd = rs.getString("password");
					String retName = rs.getString("name");
					if(!needAuth || retPwd.equals(pwd))
					{
						name = retName;
					}
			  	}
			  	rs.close();
			  	stmt.close();
			  	conn.close();
			}
		}
		catch(SQLException se)
		{
			se.printStackTrace();
	    }
	    catch(Exception e)
	    {
			e.printStackTrace();
	   	}
	   	finally
	   	{
			try
		 	{
		  		if(stmt != null)
		  		{
		  			stmt.close();
		  		} 
		  	}
		  	catch(SQLException se2)
		  	{}
		  	try
		  	{
				if(conn != null)
			 	{
			 		conn.close();
			 	}
		  	}
		  	catch(SQLException se)
		  	{
				se.printStackTrace();
		  	}
	   	}    
	   	return name;
	}

	private static void LookupHBase(String usr, TreeSet<String> friends)
	{
		HTableInterface htable = null;
		try
		{
			htable = m_htp.getTable(Bytes.toBytes(m_table));
			Result res = htable.get(new Get(Bytes.toBytes(usr)));
			if (!res.isEmpty())
			{
				for (KeyValue pair : res.list())
				{
					if (pair != null)
					{
						String tmp = new String(pair.getValue(), "UTF-8");
						String[] strs = tmp.split(",");
						for(String s : strs)
						{
							friends.add(s);
						}
					}
				}
			}
			if (htable != null)
			{
				htable.close();
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		catch(Exception e0)
		{
			e0.printStackTrace();
		}
		finally
		{
			try
			{
				if (htable != null)
				{
					htable.close();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private static ResHolder LookupDDB(String usr)
	{
		ResHolder ret = new ResHolder("", "");
		try
		{
			Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		    key.put("userid", new AttributeValue().withN(usr));
		    GetItemRequest req = new GetItemRequest()
								 .withTableName(m_dbtbl)
								 .withKey(key)
								 .withAttributesToGet(Arrays.asList("time", "url"));
		    Map<String, AttributeValue> res = m_ddb.getItem(req).getItem();
		    ret = new ResHolder(res.get("time").getS(), res.get("url").getS());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return ret;
	}
	
	private static JSONObject ProcessMySQL(Map<String, Deque<String>> params)
	{
		JSONObject response = new JSONObject();
		String uid = null;
		String pwd = null;
		if(!params.containsKey("id") || !params.containsKey("pwd") ||
		   params.get("id").size() != 1 || params.get("pwd").size() != 1 ||
		   (uid = params.get("id").peek()) == null ||
		   (pwd = params.get("pwd").peek()) == null)
		{
			response.put("err", "bad request");
		}
		else
		{
			String resp = LookupSQL(uid, pwd, true);
        	response.put("name", resp);
		}		
		return response;
	}
	
	private static JSONObject ProcessHBase(Map<String, Deque<String>> params)
	{
		JSONObject response = new JSONObject();
		TreeSet<String> frdSet = new TreeSet<String>();
		String uid = null;
		if(!params.containsKey("id") || params.get("id").size() != 1 ||
		   (uid = params.get("id").peek()) == null)
		{
			response.put("err", "bad request");
		}
		else
		{
			LookupHBase(uid, frdSet);
			Iterator<String> itr = frdSet.iterator();
			JSONArray friends = new JSONArray();
			while (itr.hasNext())
			{
				JSONObject frd = new JSONObject();
				frd.put("userid", itr.next());
				friends.add(frd);
			}
        	response.put("friends", friends);
		}	
		return response;
	}
	
	private static JSONObject ProcessDynamoDB(Map<String, Deque<String>> params)
	{
		JSONObject response = new JSONObject();
		String uid = null;
		if(!params.containsKey("id") || params.get("id").size() != 1 ||
		   (uid = params.get("id").peek()) == null)
		{
			response.put("err", "bad request");
		}
		else
		{
			ResHolder ret = LookupDDB(uid);
			response.put("time", ret.dat);
			response.put("url", ret.url);
		}	
		return response;
	}
	
	private static JSONObject AllTogether(Map<String, Deque<String>> params)
	{
		JSONObject response = new JSONObject();
		TreeSet<AllHolder> res = new TreeSet<AllHolder>();
		String uid = null;
		String pwd = null;
		if(!params.containsKey("id") || !params.containsKey("pwd") ||
		   params.get("id").size() != 1 || params.get("pwd").size() != 1 ||
		   (uid = params.get("id").peek()) == null ||
		   (pwd = params.get("pwd").peek()) == null)
		{
			response.put("err", "bad request");
		}
		else
		{
			String res1 = LookupSQL(uid, pwd, true);
			response.put("name", res1);
			if(!res1.equals("Unauthorized"))
			{
				TreeSet<String> frdSet = new TreeSet<String>();
				LookupHBase(uid, frdSet);
				Iterator<String> itr = frdSet.iterator();
				while (itr.hasNext())
				{
					String frdId = itr.next();
					String frdNm = LookupSQL(frdId, "", false);
					ResHolder res2 = LookupDDB(frdId);
					AllHolder tmpHd = new AllHolder(res2.dat, res2.url, frdNm);
					res.add(tmpHd);
				}
				
				JSONArray photos = new JSONArray();
				Iterator<AllHolder> it = res.iterator();
				while (it.hasNext())
				{
					JSONObject pt = new JSONObject();
					AllHolder tmpHd = it.next();
					pt.put("url", tmpHd.url);
					pt.put("time", tmpHd.dat);
					pt.put("name", tmpHd.nam);
					photos.add(pt);
				}
				response.put("photos", photos);
			}
		}	
		return response;
	}

    public static void main(String[] args) throws Exception
    {
        final MiniSite minisite = new MiniSite();
        final ObjectMapper mapper = new ObjectMapper();
        
        Undertow.builder()
        .addHttpListener(8080, "0.0.0.0")
        .setHandler(new HttpHandler() {

            public void handleRequest(final HttpServerExchange exchange) throws Exception {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; encoding=UTF-8");
                Sender sender = exchange.getResponseSender();
                
				
				String path = exchange.getRequestPath();
				Map<String,Deque<String>> params = exchange.getQueryParameters();
				JSONObject response = new JSONObject();
				switch(path.charAt(5))
				{
					case '1': response = ProcessMySQL(params); break;
					case '2': response = ProcessHBase(params); break;
					case '3': response = ProcessDynamoDB(params); break;
					case '4': response = AllTogether(params); break;
					default : break;
				}									
	            String content = "returnRes("+mapper.writeValueAsString(response)+")";
	            sender.send(content);			
            }
        }).build().start();
    }
}

