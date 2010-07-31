package aecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import aecs.impl.AECSFuture;
import aecs.impl.FutureStub;

import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.labs.taskqueue.Queue;
import com.google.appengine.api.labs.taskqueue.QueueFactory;
import com.google.appengine.api.labs.taskqueue.TaskOptions;
import com.google.appengine.api.labs.taskqueue.TaskOptions.Builder;

/**
 * An implementation of CompletionService using App Engine services (task queues,
 * datastore).  Note that the Future objects returned by this code may differ slightly from 
 * a normal Future object.  Specifically those returned by the submit(...) functions below are
 * stubs which poll for when the task has finished.  
 * 
 * @author jasonjones
 *
 */
public class AppEngineCompletionService<V extends Serializable> implements CompletionService<V> {

	private static final String KIND = "_AECS";
	private static final Logger logger = Logger.getLogger(AppEngineCompletionService.class.getName());
	
	private BlockingDeque<Future<V>> finishedFutures = new LinkedBlockingDeque<Future<V>>();
	private Key aecsKey;
	
	public AppEngineCompletionService() {
		Entity aecsEntity = new Entity(KIND);
		aecsKey = DatastoreServiceFactory.getDatastoreService().put(aecsEntity);
	}
	
	@Override
	public Future<V> poll() {
		while (finishedFutures.isEmpty()) {
			checkForNewFutures();
			try {
				Thread.sleep(500); // probably a better way than just looping
			} catch (InterruptedException ie) {
				// ok, just check again
			}
		}
		return finishedFutures.pollFirst();
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.CompletionService#poll(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Future<V> poll(long timeout, TimeUnit unit)
			throws InterruptedException {
		long stop = System.currentTimeMillis() + unit.toMillis(timeout);
		while (finishedFutures.isEmpty() && System.currentTimeMillis() < stop) {
			checkForNewFutures();
			Thread.sleep(500); // probably a better way than just looping and sleeping
		}
		return finishedFutures.take();
	}
	
	/**
	 * Beware -- futures returned by this method may be consumed by poll() or take() methods and never return a result
	 * You should either use poll() and take(), or use the futures returned by this method, but not both concurrently.
	 * 
	 * TODO:  Handle above scenario, or somehow prevent client from mixing these two
	 * @see java.util.concurrent.CompletionService#submit(java.util.concurrent.Callable)
	 */
	@Override
	public Future<V> submit(Callable<V> task) {
		Future<V> future = null;
		try {
			Key futureKey = new AECSFuture<V>(task, aecsKey, null).persist(); // setup datastore entry w/o value
			byte[] payload = serializeForTaskQueue(task, futureKey);
			Queue aecsQueue = QueueFactory.getQueue("aecs");		
			TaskOptions options =
				Builder.url("/_aecs")
				.payload(payload, "binary/octet-stream")
				.method(TaskOptions.Method.POST);	
			
			aecsQueue.add(options);
			future = new FutureStub<V>(futureKey);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return future;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.CompletionService#submit(java.lang.Runnable, java.lang.Object)
	 */
	@Override
	public Future<V> submit(Runnable task, V result) {
		return submit(new CallableRunner<V>(task, result));
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.CompletionService#take()
	 */
	@Override
	public Future<V> take() throws InterruptedException {
		while (finishedFutures.isEmpty()) {
			checkForNewFutures();
			Thread.sleep(500); // probably a better way than just looping
		}
		return finishedFutures.takeFirst();
	}
	
	/**
	 * Convenience method for just exeucting a runnable in a task queue
	 * 
	 * @param r
	 */
	public static void run(Runnable r) {
		AppEngineCompletionService<String> aecs = new AppEngineCompletionService<String>();
		aecs.submit(r, "");
	}
	
	private byte[] serializeForTaskQueue(Callable<V> callable, Key futureKey) throws IOException, IllegalArgumentException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(10 * 1024);
		GZIPOutputStream gos = new GZIPOutputStream(baos);
		ObjectOutputStream oos = new ObjectOutputStream(gos);
		oos.writeObject(this.aecsKey);
		oos.writeObject(futureKey);
		oos.writeObject(callable);
		oos.flush();
		oos.close();
		gos.close();
		baos.close();
		byte[] returnVal = baos.toByteArray();
		logger.info("task queue message is " + returnVal.length + " bytes long");
		if (returnVal.length > (10 * 1024)) {
			throw new IllegalArgumentException("callable is to big for task queues");
		}
		return returnVal;
	}
	
	private void checkForNewFutures() {
		try {
			List<Future<V>> futures = null;
			synchronized (this) {
				logger.info("looking for completed futures");
				futures = AECSFuture.<V>queryCompletedFutures(aecsKey, null);
			}
			if (futures != null) {
				for (Future<V> future : futures) {
					finishedFutures.addFirst(future);
				}
			}
		} catch (Exception e) {
			logger.severe("unable to query for completed futures");
			e.printStackTrace(System.err);
		}
	}

	private static class CallableRunner<V extends Serializable> implements Callable<V>, Serializable {
		private static final long serialVersionUID = -7240458786075877936L;
		
		private Runnable task;
		private V result;
		
		public CallableRunner(Runnable task, V result) {
			this.task = task;
			this.result = result;
		}
		public V call() throws Exception {
			task.run();
			return result;
		}
	}

}
