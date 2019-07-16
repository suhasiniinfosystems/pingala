package collection.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import collection.db.DbList.Item;
import collection.db.DbList.ReadCursor;
import collection.db.DbList.UpdatableCursor;
import collection.db.DbList.UpdatableCursor.InsertItem;
import collection.db.DbList.UpdatableCursor.UpdateItem;
import collection.db.DbList.VersionContainer;

public class TransactionalTable<V> {
	
	static class Util<V> {

		public void commit(Collection<InsertItem<V>> insertedItems, List<UpdateItem<V>> updatedItems, long tranId, DbList<V> dbList, CommitManager commitManager) {
			CommitList commitList = new  CommitList(dbList, insertedItems, updatedItems);
			List<CommitList> commitListColl = new ArrayList<CommitList>();
			commitListColl.add(commitList);
			CommitBatch commitBatch = new CommitBatch(commitListColl);
			VersionContainer commitId= commitManager.commit(commitBatch);
			if ( updatedItems != null ) {
				for (UpdateItem<V> updateItem : updatedItems) {
					Item<V> currentItem = updateItem.getCurrentItem();
					List<Object> keyValueList = updateItem.getRecordUniqueLockInfo().getValueList();
					List<UniqueLock> uniqueLockList = updateItem.getRecordUniqueLockInfo().getUniqueLockList();
					for (int i = 0; i < uniqueLockList.size(); i++) {
						UniqueLock uniqueLock = uniqueLockList.get(i);
						Object keyValue = keyValueList.get(i);
						uniqueLock.release(keyValue, false);
					}
					currentItem.unlockAfterUpdate(tranId, commitId);
				}
			}
			if ( insertedItems != null ) {
				for (InsertItem<V> insertedItem : insertedItems) {
					V value = insertedItem.getValue();
					List<Object> keyValueList = insertedItem.getRecordUniqueLockInfo().getValueList();
					List<UniqueLock> uniqueLockList = insertedItem.getRecordUniqueLockInfo().getUniqueLockList();
					for (int i = 0; i < uniqueLockList.size(); i++) {
						UniqueLock uniqueLock = uniqueLockList.get(i);
						Object keyValue = keyValueList.get(i);
						uniqueLock.release(keyValue, false);
					}
				}
			}
			
		}
		
	}
	
	public static class SnapshotCursor<V> {

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
	
	public static class ReadWriteCursor<V> {

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

		public void insert(V value) throws DuplicateKeysExistsException {
			updatableCursor.insert(value);
		}

		public void printNode() {
			updatableCursor.printNode();
		}
		
		public V get() {
			return updatableCursor.get();
		}

		public void update(V updatedValue) throws DuplicateKeysExistsException {
			
			updatableCursor.update(updatedValue);

		}
		
		public boolean next() {
			return updatableCursor.next();
		}

		public void commit() {
			List<UpdateItem<V>> updatedItems = updatableCursor.getUpdatedItems();
			List<InsertItem<V>> insertedItesm = updatableCursor.getInsertedItems();
			Util util = new Util();
			util.commit(insertedItesm, updatedItems, tranId, dbList, commitManager);
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
	private final List<PrimaryKey<?, V>> primaryKeysProvider;
	private final Copy<V> copier;
	
	public TransactionalTable(Copy<V> copier, Collection<V> coll, List<PrimaryKey<?, V>> primaryKeysProvider) throws DuplicateKeysExistsException {
		dbList = new DbList<V>(primaryKeysProvider);
		long tranId = nextTranId.incrementAndGet();
		
		List<InsertItem<V>> insertdList = new ArrayList<InsertItem<V>>();
		
		for (V value : coll) {
			RecordUniqueLockInfo recordUniqueLockInfo = dbList.primaryKeyCheck(value, tranId);
			InsertItem<V> insertedItem = new InsertItem<V>(value, recordUniqueLockInfo);
			insertdList.add(insertedItem);
		}
		
		Util<V> util = new Util<V>();
		util.commit(insertdList, null, tranId, dbList, commitManager);
		this.primaryKeysProvider = primaryKeysProvider;
		this.copier = copier;
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
		UpdatableCursor<V> updatableCursor = new UpdatableCursor<>(dbList, rowFilter, tranId, snapshotVersion, copier);
		
		return new ReadWriteCursor<V>(updatableCursor, commitManager, dbList, tranId, snapshotVersion);
	}

	public SnapshotCursor<V> createSnapshotCursor() {
		return createSnapshotCursor(null);
	}

	public SnapshotCursor<V> createSnapshotCursor(RowFilter<V> rowFilter) {
		VersionContainer snapshotVersion = commitManager.getLatestVersionContainer();
		ReadCursor<V> readCursor = new ReadCursor<V>(dbList.getListQueue().getHead(), rowFilter, snapshotVersion.getValue(), copier);
		SnapshotCursor<V> snapshotCursor = new SnapshotCursor<>(readCursor);
		return snapshotCursor;
	}

	
}
