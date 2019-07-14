package collection.db;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import collection.db.DbList.VersionContainer;

class CommitManager<V extends Copy<V>> {
	
	
	private static final AtomicLong commitRunningNumber = new AtomicLong(0L);
	private final ReentrantLock commitLock = new ReentrantLock(true);
	private final ConcurrentLinkedDeque<VersionContainer> versionDeque = new ConcurrentLinkedDeque<VersionContainer>();

	public VersionContainer getLatestVersionContainer() {
		return versionDeque.getLast();
	}

	public VersionContainer commit(CommitBatch commitBatch) {
		VersionContainer commitID = new VersionContainer();
		commitBatch.prepareToAdd(commitID);
		commitLock.lock();
		try {
			commitBatch.add();
			long commitId = commitRunningNumber.incrementAndGet();
			commitID.setValue(commitId);
			versionDeque.add(commitID);
		} finally {
			commitLock.unlock();
		}
		return commitID;
	}
	
}
