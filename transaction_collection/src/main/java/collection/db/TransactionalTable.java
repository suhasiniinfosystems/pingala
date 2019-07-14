package collection.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import collection.db.DbList.Item;
import collection.db.DbList.ReadCursor;
import collection.db.DbList.UpdatableCursor;
import collection.db.DbList.UpdatableCursor.UpdateItem;
import collection.db.DbList.VersionContainer;

public class TransactionalTable<V extends Copy<V>> {
	
	static class Util<V extends Copy<V>> {

		public void commit(Collection<V> adds, List<UpdateItem<V>> updatedItems, long tranId, DbList<V> dbList, CommitManager commitManager) {
			CommitList commitList = new  CommitList(dbList, adds, updatedItems);
			List<CommitList> commitListColl = new ArrayList<CommitList>();
			commitListColl.add(commitList);
			CommitBatch commitBatch = new CommitBatch(commitListColl);
			VersionContainer commitId= commitManager.commit(commitBatch);
			if ( updatedItems != null ) {
				for (UpdateItem<V> updateItem : updatedItems) {
					Item<V> currentItem = updateItem.getCurrentItem();
					currentItem.unlockAfterUpdate(tranId, commitId);
				}
			}
		}
		
	}
	
	public static class SnapshotCursor<V extends Copy<V>> {

		ReadCursor<V> readCursor;
		
		public SnapshotCursor(ReadCursor<V> readCursor) {
			this.readCursor = readCursor;
		}

		public void printNode() {
			readCursor.printNode();
		}

		public boolean next() {
			return readCursor.next();
		}

		public V get() {
			return readCursor.get();
		}
		
	}

	
	
	public static class ReadWriteCursor<V extends Copy<V>> {

		private final UpdatableCursor<V> updatableCursor;
		private final DbList<V> dbList;
		private final CommitManager<?> commitManager;
		private final long tranId;
		
		public ReadWriteCursor(UpdatableCursor<V> updatableCursor, CommitManager<?> commitManager, DbList<V> dbList, long tranId, VersionContainer snapshotVersion) {
			this.updatableCursor = updatableCursor;
			this.commitManager = commitManager;
			this.dbList = dbList;
			this.tranId = tranId;
		}

		public void add(V value) {
			updatableCursor.add(value);
		}

		public void printNode() {
			updatableCursor.printNode();
		}
		
		public V get() {
			return updatableCursor.get();
		}

		public void update(V updatedValue) {
			updatableCursor.update(updatedValue);

		}
		
		public boolean next() {
			return updatableCursor.next();
		}

		public void commit() {
			List<UpdateItem<V>> updatedItems = updatableCursor.getUpdatedItems();
			List<V> adds = updatableCursor.getAddedItems();
			Util util = new Util();
			util.commit(adds, updatedItems, tranId, dbList, commitManager);
		}
		
		public void rollback() {
			List<UpdateItem<V>> updatedItems = updatableCursor.getUpdatedItems();
			for (UpdateItem<V> updateItem : updatedItems) {
				Item<V> currentItem = updateItem.getCurrentItem();
				currentItem.unlockAfterRollback(tranId);
			}
		}	
		
	}

	private final DbList<V> dbList;
	private final CommitManager commitManager = new CommitManager();
	private final AtomicLong nextTranId = new AtomicLong(0);
	
	public TransactionalTable(Collection<V> coll) {
		dbList = new DbList<V>();
		long tranId = nextTranId.incrementAndGet();
		Util<V> util = new Util<V>();
		util.commit(coll, null, tranId, dbList, commitManager);
	}

	public void dump() {
		dbList.dump();
	}
	
	public ReadWriteCursor<V> createReadWriteCursor() {
		return createReadWriteCursor(null);
	}

	public ReadWriteCursor<V> createReadWriteCursor(RowFilter<V> rowFilter) {
		long tranId = nextTranId.incrementAndGet();
		VersionContainer snapshotVersion = commitManager.getLatestVersionContainer();
		UpdatableCursor<V> updatableCursor = new UpdatableCursor<>(dbList, rowFilter, tranId, snapshotVersion);
		return new ReadWriteCursor<V>(updatableCursor, commitManager, dbList, tranId, snapshotVersion);
	}

	public SnapshotCursor<V> createSnapshotCursor() {
		return createSnapshotCursor(null);
	}

	public SnapshotCursor<V> createSnapshotCursor(RowFilter<V> rowFilter) {
		VersionContainer snapshotVersion = commitManager.getLatestVersionContainer();
		ReadCursor<V> readCursor = new ReadCursor<V>(dbList.getListQueue().getHead(), rowFilter, snapshotVersion.getValue());
		SnapshotCursor<V> snapshotCursor = new SnapshotCursor<>(readCursor);
		return snapshotCursor;
	}

	
}
