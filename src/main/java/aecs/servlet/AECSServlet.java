package aecs.servlet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import aecs.impl.AECSFuture;

import com.google.appengine.api.datastore.Key;


public class AECSServlet extends HttpServlet {
	
	private static final Logger logger = Logger.getLogger(AECSServlet.class.getName());
	
	@SuppressWarnings("unchecked")
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			byte[] buf = new byte[10 * 1024];
			ServletInputStream sis = req.getInputStream();
			GZIPInputStream gis = new GZIPInputStream(sis);
			ObjectInputStream ois = new ObjectInputStream(gis);
			Key aecsKey = (Key)ois.readObject();
			Key futureKey = (Key)ois.readObject();
			Callable callable = (Callable)ois.readObject();
			logger.info("processing callable for " + aecsKey);
			AECSFuture future = new AECSFuture(callable, aecsKey, futureKey);
			future.run(); 
			future.persist();
		} catch (Exception e) {
			throw new ServletException(e);
		}
	}

}
