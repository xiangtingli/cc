import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class Coordinator extends Verticle {

	//Default mode: Strongly consistent. Possible values are "strong" and "causal"
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-52-4-171-42.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-52-1-149-191.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-52-4-207-1.compute-1.amazonaws.com";

	//
	private static class LockWrapper
	{
		public Lock lock;
		public PriorityBlockingQueue<Long> timestamps = new PriorityBlockingQueue<Long>();
		
		public LockWrapper(long _timestamp)
		{
			lock = new ReentrantLock();
			timestamps.offer(_timestamp);
		}
		
		public synchronized void putTimestamp(long _timestamp)
		{
			timestamps.offer(_timestamp);
		}
		
		public synchronized void discardTimestamp()
		{
			if(!timestamps.isEmpty())
			{
				timestamps.poll();
			}
		}
		
		public synchronized boolean isMin(long _timestamp)
		{
			/*if(timestamps.isEmpty())
			{
				return true;
			}*/ //never empty
			return _timestamp <= timestamps.peek();
		}
	}
	private static ConcurrentHashMap<String, LockWrapper> lockMap = new ConcurrentHashMap<String, LockWrapper>();
	//private static ConcurrentHashMap<String, Lock> lockMap = new ConcurrentHashMap<String, Lock>();
	//private static BlockingQueue<Runnable> taskQ = new PriorityBlockingQueue<Runnable>();
	//private static ThreadPoolExecutor m_exctor = new ThreadPoolExecutor(20, 100, 5000L, TimeUnit.MILLISECONDS, taskQ);
	private static ScheduledThreadPoolExecutor m_exctor = new ScheduledThreadPoolExecutor(30);
		
	private static class Request implements Runnable, Comparable<Request>
	{ 
		public long m_timestamp;
		public String m_key;
		public int m_loc;
		public String m_value;
		protected boolean m_isGet;
		protected HttpServerRequest m_req;
		
		public Request(HttpServerRequest _req, long _timestamp, String _key, int _loc)
		{
			m_timestamp = _timestamp;
			m_key = _key;
			m_loc = _loc;
			m_value = null;
			m_isGet = true;
			m_req = _req;
		}
		
		public Request(HttpServerRequest _req, long _timestamp, String _key, String _value)
		{
			m_timestamp = _timestamp;
			m_key = _key;
			m_loc = 0;
			m_value = _value;
			m_isGet = false;
			m_req = _req;
		}		
		
		private synchronized String Get() { return "0"; }
		private synchronized void Put() {}
		
		@Override
		public int compareTo(Request other)
		{
			if(m_timestamp != other.m_timestamp)
			{
				return m_timestamp > other.m_timestamp ? 1 : -1;
			}
			return 0;
		}
		
		@Override
		public void run() {}
	}
	
	private static class SCRequest extends Request 
	{	
		public SCRequest(HttpServerRequest _req, long _timestamp, String _key, int _loc)
		{
			super(_req, _timestamp, _key, _loc);
		}
		
		public SCRequest(HttpServerRequest _req, long _timestamp, String _key, String _value)
		{
			super(_req, _timestamp, _key, _value);
		}	
	
		@Override
		public void run()
		{
			if(m_isGet)
			{
				String res = Get();
				m_req.response().end(res);
			}
			else
			{
				Put();
			}
		}
			
		private synchronized String Get()
		{
			if(lockMap.containsKey(m_key))
		    {
		    	LockWrapper lw = lockMap.get(m_key);
		    	lw.putTimestamp(m_timestamp);
		    	while(true)
		    	{
		    		if(lw.isMin(m_timestamp) && lw.lock.tryLock())
		    		{
		    			break;
		    		}
		    		else
		    		{
						try
						{
							Thread.sleep(10);
						}
						catch (InterruptedException e) {}
		    		}
		    	}
		    	lw.lock.unlock();
				lw.discardTimestamp();
		    	//notifyAll();
		    }
		    String res = "0";
		    try
		    {
		    	String dc = null;
		    	switch(m_loc)
		    	{
		    		case 1: dc = dataCenter1; break;
		    		case 2: dc = dataCenter2; break;
		    		case 3: dc = dataCenter3; break;
		    		default: break;
		    	}
		    	res = KeyValueLib.GET(dc, m_key);
		    	System.out.println("GET: " + m_key + " : " + res + " @ " + m_timestamp);
		    } catch(IOException e) {}
		    return res;
		}
			
		private synchronized void Put()
		{
		    if(!lockMap.containsKey(m_key))
		    {
		    	LockWrapper lw = new LockWrapper(m_timestamp);
		    	lw.lock.lock();
		    	lockMap.putIfAbsent(m_key, lw);
		    	try
				{
					System.out.println("PUT: " + m_key + " : " + m_value + " @ " + m_timestamp);
					KeyValueLib.PUT(dataCenter1, m_key, m_value);
					KeyValueLib.PUT(dataCenter2, m_key, m_value);
					KeyValueLib.PUT(dataCenter3, m_key, m_value);
				}
				catch(IOException e){}
				finally
				{
					lw.lock.unlock();
					lw.discardTimestamp();
		    		//notifyAll();
				}
		    }
		    else
		    {
		    	LockWrapper lw = lockMap.get(m_key);
		    	lw.putTimestamp(m_timestamp);
		    	while(true)
		    	{
		    		if(lw.isMin(m_timestamp) && lw.lock.tryLock())
		    		{
		    			break;
		    		}
		    		else
		    		{
						try
						{
							Thread.sleep(1);
						}
						catch (InterruptedException e) {}
					}
		    	}
		    	try
				{
					System.out.println("PUT: " + m_key + " : " + m_value + " @ " + m_timestamp);
					KeyValueLib.PUT(dataCenter1, m_key, m_value);
					KeyValueLib.PUT(dataCenter2, m_key, m_value);
					KeyValueLib.PUT(dataCenter3, m_key, m_value);
				}
				catch(IOException e){}
				finally
				{
					lw.lock.unlock();
					lw.discardTimestamp();
		    		//notifyAll();
				}
		    }
		}
	}
	
	private static class CCRequest extends Request 
	{	
		public CCRequest(HttpServerRequest _req, long _timestamp, String _key, int _loc)
		{
			super(_req, _timestamp, _key, _loc);
		}
		
		public CCRequest(HttpServerRequest _req, long _timestamp, String _key, String _value)
		{
			super(_req, _timestamp, _key, _value);
		}	
	
		@Override
		public void run()
		{
			if(m_isGet)
			{
				String res = Get();
				m_req.response().end(res);
			}
			else
			{
				Put();
			}
		}
			
		private String Get()
		{
		    String res = "0";
		    try
		    {
		    	String dc = null;
		    	switch(m_loc)
		    	{
		    		case 1: dc = dataCenter1; break;
		    		case 2: dc = dataCenter2; break;
		    		case 3: dc = dataCenter3; break;
		    		default: break;
		    	}
		    	res = KeyValueLib.GET(dc, m_key);
		    	//System.out.println("GET: " + m_key + " : " + res + " @ " + m_timestamp);
		    } catch(IOException e) {}
		    return res;
		}
			
		private void Put()
		{
	    	try
			{
				//System.out.println("PUT: " + m_key + " : " + m_value + " @ " + m_timestamp);
				KeyValueLib.PUT(dataCenter1, m_key, m_value);
				KeyValueLib.PUT(dataCenter2, m_key, m_value);
				KeyValueLib.PUT(dataCenter3, m_key, m_value);
			}
			catch(IOException e){}
		}
	}
	
	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				//You may use the following timestamp for ordering requests
                final long timestamp = new Timestamp(System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).getTime();
				if(consistencyType.equals("strong"))
				{
					m_exctor.submit(new SCRequest(req, timestamp, key, value));
				}
				else
				{
					m_exctor.submit(new CCRequest(req, timestamp, key, value));
				}
				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				//You may use the following timestamp for ordering requests
                final long timestamp = new Timestamp(System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).getTime();
				if(consistencyType.equals("strong"))
				{
					m_exctor.submit(new SCRequest(req, timestamp, key, Integer.parseInt(loc)));
				}
				else
				{
					m_exctor.submit(new CCRequest(req, timestamp, key, Integer.parseInt(loc)));
				}
			}
		});

		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
                        @Override
                        public void handle(final HttpServerRequest req) {
                                MultiMap map = req.params();
                                consistencyType = map.get("consistency");
                                //This endpoint will be used by the auto-grader to set the 
								//consistency type that your key-value store has to support.
                                //You can initialize/re-initialize the required data structures here
                                req.response().end();
                        }
                });

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}
