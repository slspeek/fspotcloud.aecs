package aecs.test;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Sums a range of numbers (inclusive)
 * 
 * @author jasonjones
 *
 * @param <Integer>
 */
public class SumCallable implements Callable<Integer>, Serializable {

	private static final long serialVersionUID = -6880859505734011751L;
	
	private int start;
	private int end;

	public SumCallable(int start, int end) {
		this.start = start;
		this.end = end;
	}
	
	@Override
	public Integer call() throws Exception {
		int returnVal = 0;
		for (int i=start;i<=end;i++) {
			returnVal += i;
		}
		return returnVal;
	}

}
