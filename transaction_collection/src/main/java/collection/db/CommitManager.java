package collection.db;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import collection.db.DbList.VersionContainer;

class CommitManager<V> {
	
	
	private static final AtomicLong commitRunningNumber = new AtomicLong(0L);
	private final ReentrantLock commitLock = new ReentrantLock(true);
	private final ConcurrentLinkedDeque<VersionContainer> versionDeque = new ConcurrentLinkedDeque<VersionContainer>();

	public VersionContainer getLatestVersionContainer() {
		VersionContainer versionContainer = new VersionContainer();
		versionContainer.setValue(versionDeque.getLast().getValue());
		return versionContainer;
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
