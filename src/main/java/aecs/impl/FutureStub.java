/**
 * 
 */
package aecs.impl;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;


import com.google.appengine.api.datastore.Key;

public class FutureStub<V extends Serializable> implements Future<V> {
	private static final Logger logger = Logger.getLogger(FutureStub.class.getName());
	private Key key;
	
	public FutureStub(Key key) {
		this.key = key;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		V returnVal = null;
		while (returnVal == null) {
			try {
				returnVal = AECSFuture.<V>queryFutureValue(key, null);
			} catch (Exception e) {
				logger.severe("unable to query for completed future " + key);
				e.printStackTrace(System.err);
				break; // right to return null instead of coughing up an exception?
			}
			if (returnVal == null) {
				Thread.sleep(500);
			}
		}
		return returnVal;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException { 
		long stop = System.currentTimeMillis() + unit.toMillis(timeout);
		V returnVal = null;
		while (returnVal == null && System.currentTimeMillis() < stop) {
			try {
				returnVal = AECSFuture.<V>queryFutureValue(key, null);
			} catch (Exception e) {
				logger.severe("unable to query for completed future " + key);
				e.printStackTrace(System.err);
				break; // right to return null instead of coughing up an exception?
			}
			if (returnVal == null) {
				Thread.sleep(500);
			}
		}
		return returnVal;
	}

	// TODO: probably could support cancellation this somehow

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) { throw new UnsupportedOperationException(); }
	@Override
	public boolean isCancelled() { throw new UnsupportedOperationException(); }

	@Override
	public boolean isDone() { throw new UnsupportedOperationException();  }
}