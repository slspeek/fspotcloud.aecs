package aecs.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

public class AECSFuture<V extends Serializable> extends FutureTask<V> {
	
	private static final Logger logger = Logger.getLogger(AECSFuture.class.getName());
	public static final String KIND = "_AECSFuture";

	private Key aecsKey;
	private Key futureKey;
	
	public AECSFuture(Callable<V> callable, Key aecsKey, Key futureKey) {
		super(callable);
		this.aecsKey = aecsKey;
		this.futureKey = futureKey;
	}
	
	public Key persist() throws Exception {
		DatastoreService service = DatastoreServiceFactory.getDatastoreService();
		Entity aecsFutureEntity = (futureKey == null ? new Entity(KIND, aecsKey): service.get(futureKey));
		if (this.isDone()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(this.get());
			oos.close();
			baos.close();
			byte[] serializedValue = baos.toByteArray();
			aecsFutureEntity.setUnindexedProperty("value", new Blob(serializedValue));
		}
		return DatastoreServiceFactory.getDatastoreService().put(aecsFutureEntity);			
	}
	
	/**
	 * Query datastore and consume futures which are stored there.  This means caller *must* process these futures --
	 * they are consumed by this method
	 * 
	 * @param <K> class of return result
	 * @param aecsGUID AECS to use
	 * @param tx transaction (optional) to use in querying and deleting
	 * @return List of Futures which contain results -- do not try to run these futures
	 */
	@SuppressWarnings("unchecked")
	public static <K> List<Future<K>> queryCompletedFutures(Key aecsKey, Transaction tx) throws Exception {
		List<Future<K>> returnVal = new ArrayList<Future<K>>();
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		boolean commitAtEnd = false;
		if (tx == null) {
			tx = datastore.beginTransaction();
			commitAtEnd = true;
		}
		try {
			Query query = new Query(KIND, aecsKey);
			List<Entity> entities = datastore.prepare(tx, query).asList(FetchOptions.Builder.withLimit(1000));
			for (Entity aecsFutureEntity : entities) {
				if (aecsFutureEntity.hasProperty("value")) {
					Object value = deserialize((Blob)aecsFutureEntity.getProperty("value"));
					Future<K> result = new ResultFuture<K>((K)value);
					returnVal.add(result);
					datastore.delete(aecsFutureEntity.getKey());
				}
			}
		} catch (Exception e) {
			if (commitAtEnd) { tx.rollback(); }
			throw e;
		}
		if (commitAtEnd) {
			tx.commit();
		}
		return returnVal;
	}
	
	/**
	 * Query datastore to see if value is there for given future.  If value is found the future is consumed
	 * 
	 * @param <K>
	 * @param futureKey
	 * @param tx
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static <K> K queryFutureValue(Key futureKey, Transaction tx) throws Exception {
		K returnVal = null;
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		boolean commitAtEnd = false;
		if (tx == null) {
			tx = datastore.beginTransaction();
			commitAtEnd = true;
		}
		try {
			// TODO: optimize using an indexed "isDone" property
			Entity future = datastore.get(tx, futureKey);
			if (future.hasProperty("value")) {
				returnVal = (K)deserialize((Blob)future.getProperty("value"));
				datastore.delete(tx, futureKey);
			}
		} catch (Exception e) {
			if (commitAtEnd) { tx.rollback(); }
			throw e;
		}
		if (commitAtEnd) {
			tx.commit();
		}
		return returnVal;
	}
	
	private static Object deserialize(Blob blob) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bais = new ByteArrayInputStream(blob.getBytes());
		ObjectInputStream ois = new ObjectInputStream(bais);
		return ois.readObject();
	}
	
	/**
	 * This is a bucket to return the results in -- still searching for a better way to handle
	 * 
	 */
	private static class ResultFuture<U> implements Future<U> {
		
		private U result;
		
		public ResultFuture(U result) {
			this.result = result;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) { throw new UnsupportedOperationException(); }

		@Override
		public U get() throws InterruptedException, ExecutionException {
			return result;
		}

		@Override
		public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException { throw new UnsupportedOperationException(); }

		@Override
		public boolean isCancelled() { throw new UnsupportedOperationException(); }

		@Override
		public boolean isDone() { return true; }
		
	}
	
	
}
