package collection.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class UniqueLock {

	final Set<Object> set;

	final Map<Object, Long> tranLockMap = new HashMap<Object, Long>();
	private final Map<Object, Condition> conditionMap = new HashMap<Object, Condition>();
	private final ReentrantLock reentrantLock = new ReentrantLock();

	public static RecordUniqueLockInfo lockForUniqueKey( Long tranId, List<String> keys, List<Object> values, List<UniqueLock> uniqueLocks) throws DuplicateKeysExistsException {

		RecordUniqueLockInfo recordUniqueLockInfo = new RecordUniqueLockInfo();

		for (int i = 0; i < keys.size(); i++) {
			//String keyName = keys.get(i);
			Object columnValue = values.get(i);
			UniqueLock uniqueLock = uniqueLocks.get(i);
			
			boolean noDuplicates = uniqueLock.lockIfNoDuplicates(columnValue, tranId);
			if (!noDuplicates) {
				List<UniqueLock> uniqueLockList = recordUniqueLockInfo.getUniqueLockList();
				List<Object> valueList = recordUniqueLockInfo.getValueList();
				for (int j = 0; j < uniqueLockList.size(); j++) {
					UniqueLock uniqueLock2 = uniqueLockList.get(j);
					Object key = valueList.get(j);
					uniqueLock2.release(key, true);
				}
				throw new DuplicateKeysExistsException("Duplicate key exists exception");
			}
			recordUniqueLockInfo.getUniqueLockList().add(uniqueLock);
			recordUniqueLockInfo.getValueList().add(columnValue);
		}
		return recordUniqueLockInfo;
		
	}
	
	UniqueLock() {
		this(new HashSet<Object>());
	}

	UniqueLock(Set<Object> set) {
		this.set = set;
	}

	public boolean lockIfNoDuplicates(Object key, Long tranId) {
	
		reentrantLock.lock(); 
		//System.out.println("Entering unique lock");
		try {
			boolean noDuplicates = false;
			while ( true ) {
				if ( set.contains(key)) {
					break;
				}
				Long tranId0 = tranLockMap.get(key);
				if ( tranId0 == null ) {
					noDuplicates = true;
					break;
				}
				if ( tranId0.longValue() == tranId.longValue() ) {
					break;
				}
				Condition condition = conditionMap.get(key);
				if ( condition == null ) {
					condition = reentrantLock.newCondition();
					conditionMap.put(key, condition);
				}
				try {
					condition.await();
				} catch (Exception e) {}
			}
			
			if ( noDuplicates ) {
				tranLockMap.put(key, tranId);
			}
			return noDuplicates;
			
		} finally {
			//System.out.println("Exiting unique lock");
			reentrantLock.unlock();
		}
	}

	public void release(Object key, boolean rollback) {
		reentrantLock.lock();
		//System.out.println("Releasing unique lock");
		try {
			// remove in all conditions
			tranLockMap.remove(key);
			if ( !rollback ) {
				set.add(key);
			}
			Condition condition = conditionMap.get(key);
			if ( condition != null ) {
				condition.signal();
				if ( reentrantLock.getWaitQueueLength(condition) == 0 ) {
					Object removed = conditionMap.remove(key);
					System.out.println(Thread.currentThread().getName() + " - Deleting the condition object : " + key + " / "+ removed);
				}
			}
		} finally {
			reentrantLock.unlock();
		}
		
	}

	static class Runnable1 implements Runnable {
	
		private UniqueLock uniqueLock;
		private long startDelay;
		private long keepLockDUpation;
		private String key;
		boolean rollback;
		
		public Runnable1(UniqueLock uniqueLock, long startDelay, long keepLockDUpation, String key, boolean rollback) {
			this.uniqueLock = uniqueLock;
			this.startDelay = startDelay;
			this.keepLockDUpation = keepLockDUpation;
			this.key = key;
			this.rollback = rollback;
		}
		
		@Override
		public void run() {
			try {
				long tranId = 1;
				Thread.sleep(startDelay);
				System.out.println(Thread.currentThread().getName() + " - try lock..." );
				boolean locked = uniqueLock.lockIfNoDuplicates(key, tranId);
				if ( !locked ) {
					System.out.println(Thread.currentThread().getName() + " - lock failed..." );
				} else {
					System.out.println(Thread.currentThread().getName() + " - lock acquired..." );
					Thread.sleep(keepLockDUpation);
					System.out.println(Thread.currentThread().getName() + " - releasing lock with rollback = " + rollback);
					uniqueLock.release(key, rollback);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}

	}

	public static void main(String[] args) throws Exception {
		main3(args);
	}
	
	public static void main1(String[] args) throws Exception {
		
		final UniqueLock uniqueLock = new UniqueLock();
		Runnable1 runnable1 = new Runnable1(uniqueLock, 0, 10000, "Sanjay", false);
		Runnable1 runnable2 = new Runnable1(uniqueLock, 1000, 10000, "Sanjay2", false);
		
		Thread thread1 = new Thread(runnable1, "Thread-1");
		Thread thread2 = new Thread(runnable2, "Thread-2");

		thread1.start();
		thread2.start();
		
		thread1.join();
		thread2.join();
		
		System.out.println(Thread.currentThread().getName() + " - set = " + uniqueLock.set);
		System.out.println(Thread.currentThread().getName() + " - lock set = " + uniqueLock.tranLockMap.keySet());
		System.out.println(Thread.currentThread().getName() + " - condition map = " + uniqueLock.conditionMap.keySet());

	}

	public static void main2(String[] args) throws Exception {
		
		final UniqueLock uniqueLock = new UniqueLock();
		Runnable1 runnable1 = new Runnable1(uniqueLock, 0, 10000, "Sanjay", false);
		Runnable1 runnable2 = new Runnable1(uniqueLock, 100, 10000, "Sanjay", false);
		Runnable1 runnable3 = new Runnable1(uniqueLock, 200, 10000, "Sanjay", false);
		
		Thread thread1 = new Thread(runnable1);
		Thread thread2 = new Thread(runnable2);
		Thread thread3 = new Thread(runnable3);

		thread1.start();
		thread2.start();
		thread3.start();
		
		thread1.join();
		thread2.join();
		thread3.join();
		
		System.out.println(Thread.currentThread().getName() + " - set = " + uniqueLock.set);
		System.out.println(Thread.currentThread().getName() + " - lock set = " + uniqueLock.tranLockMap.keySet());
		System.out.println(Thread.currentThread().getName() + " - condition map = " + uniqueLock.conditionMap.keySet());
		
	}

	public static void main3(String[] args) throws Exception {
		
		final UniqueLock uniqueLock = new UniqueLock();
		Runnable1 runnable1 = new Runnable1(uniqueLock, 0, 10000, "Sanjay", false);
		Runnable1 runnable2 = new Runnable1(uniqueLock, 100, 10000, "Sanjay", false);
		
		Thread thread1 = new Thread(runnable1);
		Thread thread2 = new Thread(runnable2);

		thread1.start();
		thread2.start();
		
		thread1.join();
		thread2.join();

		System.out.println(Thread.currentThread().getName() + " - set = " + uniqueLock.set);
		System.out.println(Thread.currentThread().getName() + " - lock set = " + uniqueLock.tranLockMap.keySet());
		System.out.println(Thread.currentThread().getName() + " - condition map = " + uniqueLock.conditionMap.keySet());
		

	}

	
}
