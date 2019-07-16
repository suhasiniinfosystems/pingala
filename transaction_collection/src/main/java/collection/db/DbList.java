package collection.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


class DbList<V> {
	
	static class ReadCursor<V> {

		private ItemNode<V> mNode;
		private final long snapshotVersion;
		private final RowFilter<V> rowFilter;
		private final Copy<V> copier;
		
		public ReadCursor(ItemNode<V> head, RowFilter<V> rowFilter, long snapshotVersion, Copy<V> copier) {
			this.mNode = head;
			this.rowFilter = rowFilter;
			this.snapshotVersion = snapshotVersion;
			this.copier = copier;
		}

		public void printNode() {
			Item<V> item = mNode.getItem();
			V value = get();
			System.out.println(" rowId : " + item.getRowId() + ", commitId : " + item.getCommitId() + ", nextVersionId : " + item.getNextVersion() + " [ " + value.toString() + " ]");
		}

		public boolean next() {

			ItemNode<V> node = mNode;
			while ( true ) {
				node = node.next();
				if ( node == null ) {
					return false;
				}
				Item<V> item = node.getItem();
				long version = item.getCommitId();
				if ( version > snapshotVersion ) {
					return false;
				} else if ( version == snapshotVersion ) {
					boolean isMatching = isMatching(item, rowFilter);
					if ( isMatching ) {
						this.mNode = node;
						return true;
					}
				} else if ( version < snapshotVersion ) {
					long nextVersion = item.getNextVersion();
					if ( nextVersion == 0 ) {
						boolean isMatching = isMatching(item, rowFilter);
						if ( isMatching ) {
							this.mNode = node;
							return true;
						}
					} else if ( nextVersion > snapshotVersion ) {
						boolean isMatching = isMatching(item, rowFilter);
						if ( isMatching ) {
							this.mNode = node;
							return true;
						}
					}
				}
			}
		}

		private synchronized boolean isMatching(Item<V> item, RowFilter<V> rowFilter) {
			V value = item.getValue();
			if ( rowFilter == null || rowFilter.accept(value)) {
				return true;
			} else {
				return false;
			}
		}
		
		public V get() {
			V value = mNode.getItem().getValue();
			V copy = copier.copy(value);
			return copy;
		}
		
	}
	
	static class UpdatableCursor<V> {

		static public class UpdateItem<V> {
			private Item<V> currentItem;
			private V nextValue;
			private RecordUniqueLockInfo recordUniqueLockInfo;
			
			public UpdateItem(Item<V> currentItem, V nextValue, RecordUniqueLockInfo recordUniqueLockInfo) {
				this.currentItem = currentItem;
				this.nextValue = nextValue;
				this.recordUniqueLockInfo = recordUniqueLockInfo;
			}
			public Item<V> getCurrentItem() {
				return currentItem;
			}
			
			public RecordUniqueLockInfo getRecordUniqueLockInfo() {
				return recordUniqueLockInfo;
			}	

			public V getNextValue() {
				return nextValue;
			}
		}

		static public class InsertItem<V> {
			private V value;
			private RecordUniqueLockInfo recordUniqueLockInfo;
			
			public InsertItem(V value, RecordUniqueLockInfo recordUniqueLockInfo) {
				this.value = value;
				this.recordUniqueLockInfo = recordUniqueLockInfo;
			}

			public RecordUniqueLockInfo getRecordUniqueLockInfo() {
				return recordUniqueLockInfo;
			}	

			public V getValue() {
				return value;
			}
		}

		private final DbList<V> dbList;
		private ItemNode<V> mNode;
		private V copy;
		
		private List<UpdateItem<V>> updates = new ArrayList<UpdateItem<V>>();
		private List<InsertItem<V>> adds = new ArrayList<InsertItem<V>>();
		
		private VersionContainer snapshotVersion;
		private final long tranId;
		private final RowFilter<V> rowFilter;
		private final Copy<V> copier;
		
		public UpdatableCursor(DbList<V> dbList, RowFilter<V> rowFilter, long tranId, VersionContainer snapshotVersion, Copy<V> copier) {
			this.dbList = dbList;
			this.mNode = dbList.getListQueue().getHead();
			this.tranId = tranId;
			this.snapshotVersion = snapshotVersion;
			this.rowFilter = rowFilter;
			this.copier = copier;
		}

		public void insert(V value) throws DuplicateKeysExistsException {
			RecordUniqueLockInfo recordUniqueLockInfo = dbList.primaryKeyCheck(value, tranId);
			InsertItem<V> insertItem = new InsertItem<V>(value, recordUniqueLockInfo);
			adds.add(insertItem);
		}

		public void printNode() {
			Item<V> item = mNode.getItem();
			V value = get();
			System.out.println(" rowId : " + item.getRowId() + ", commitId : " + item.getCommitId() + ", nextVersionId : " + item.getNextVersion() + " [ " + value.toString() + " ]");
		}
		
		public V get() {
			V value = mNode.getItem().getValue();
			copy = copier.copy(value);
			return copy;
		}

		public void update(V updatedValue) throws DuplicateKeysExistsException {
			// Primary key check 
			RecordUniqueLockInfo recordUniqueLockInfo = dbList.primaryKeyCheck(updatedValue, tranId);
			
			ItemNode<V> node = mNode;
			if ( node == null || mNode.getItem() == null ) {
				throw new RuntimeException("Invalid update request");
			} 	
			Item<V> item = mNode.getItem();
			UpdateItem<V> updateItem = new UpdateItem<V>(item, updatedValue, recordUniqueLockInfo);
			updates.add(updateItem);
		}
		
		public boolean next() {

			ItemNode<V> node = mNode;
			while ( true ) {
				node = node.next();
				if ( node == null ) {
					return false;
				}
				Item<V> item = node.getItem();
				long version = item.getCommitId();
				if ( version > snapshotVersion.getValue() ) {
					return false;
				} else if ( version <= snapshotVersion.getValue() ) {
					boolean matched = item.match(rowFilter, snapshotVersion, tranId);
					if ( matched ) {
						this.mNode = node;
						return true;
					}
				}
			}
		}

		List<UpdateItem<V>> getUpdatedItems() {
			return updates;
		}

		List<InsertItem<V>> getInsertedItems() {
			return adds;
		}

	}

	@SuppressWarnings("all")
	public static class ListQueue<V> {

		@SuppressWarnings("rawtypes")
		private volatile ItemNode head = new ItemNode(null);
		private volatile ItemNode tail = head;
		
		public ItemNode<V> getHead() {
			return head;
		}

		public ItemNode<V> getTail() {
			return tail; 
		}

		public void setTail(ItemNode<V> tail) {
			this.tail = tail;
		}
	 
		/*
		 * For test only
		 */
		public ItemNode<V> add(Item<V> row) {
			ItemNode<V> node = new ItemNode<V>(row);
			tail.setNext(node);
			tail = node;
			return node;
			
		}

		public synchronized void addNodes(ListQueue other) {
			ItemNode last = tail;
			tail = other.getTail();
			last.setNext(other.getHead().next());
		}

		public int size() {
			int size = 0;
			ItemNode node = head.next();
			while ( node != null ) {
				node = node.next();
				size++;
			}
			return size;
		}

		public void print() {
			ItemNode node = head.next();
			while ( node != null ) {
				Item<V> item = node.getItem();
				V value = item.getValue();
				System.out.println(" rowId : " + item.getRowId() + ", commitId : " + item.getCommitId() + ", nextVersionId : " + item.getNextVersion() + " [ " + value.toString() + " ]");
				node = node.next();
			}
			
		}
		
	}
	
	public static class VersionContainer {

		private volatile long value;

		public long getValue() {
			return value;
		}

		public void setValue(long value) {
			this.value = value;
		}
		
		@Override
		public String toString() {
			return String.valueOf(value);
		}
		
	}
	
	public static class ItemNode<V> {
		
		private Item<V> item;
		private volatile ItemNode<V> next;

		public ItemNode(Item<V> item) {
			this.item = item;
		}

		public Item<V> getItem() {
			return item;
		}
		
		public void setNext(ItemNode<V> next) {
			this.next = next;
		}

		public ItemNode<V> next() {
			return next;
		}

	}

	public static class Item<V> {

		private final long rowId;
		private final V value;
		private final VersionContainer commitID;

		private volatile long tranId; 

		private volatile VersionContainer nextVersion;

		public Item(long rowId, V value, VersionContainer commitID) {
			this.rowId = rowId;
			this.value = value;
			this.commitID = commitID;
		}
		
		public long getRowId() {
			return rowId;
		}

		public long getCommitId() {
			return commitID.getValue();
		}

		public boolean match(RowFilter<V> rowFilter, VersionContainer snapshotVersion, long tranId) {
			
			boolean matches = ( rowFilter == null || rowFilter.accept(value));
			if ( !matches ) {
				return false;
			}
			
			long nextVersion = getNextVersion();
			
			if ( nextVersion > 0 ) {
				if ( nextVersion > snapshotVersion.getValue() ) {
					snapshotVersion.setValue(nextVersion);
				}
				return false;
			}	

			synchronized ( this ) {
				System.out.println(Thread.currentThread().getName() + " - nextVer : " + nextVersion);
				while ( this.tranId != 0 && this.tranId != tranId ) {
					try {
						wait();
					} catch (Exception e) {
					}
				}
				// Recheck
				nextVersion = getNextVersion();
				if ( nextVersion > 0 ) {
					if ( nextVersion > snapshotVersion.getValue() ) {
						snapshotVersion.setValue(nextVersion);
					}
					notify();
					return false;
				} else {
					System.out.println(Thread.currentThread().getName() + " - Matched item.rowId : " + rowId + ", commitID : " + commitID.getValue() + ", nextVer : " + nextVersion);
					this.tranId = tranId;
					return true;							
				}
			}
		
		}
		
		public synchronized void unlockAfterUpdate(long tranId, VersionContainer nextCommitID) {
			if ( this.tranId != tranId ) {
				throw new RuntimeException("Invalid state exception");
			}
			this.tranId = 0;
			System.out.println(Thread.currentThread().getName() + " - Unlock before item.rowId : " + rowId + ", commitID : " + commitID + ", nextVer : " + nextVersion);
			this.nextVersion = nextCommitID;
			System.out.println(Thread.currentThread().getName() + " - Unlock after item.rowId : " + rowId + ", commitID : " + commitID + ", nextVer : " + nextVersion);
			notify();
		}

		public synchronized void unlockAfterRollback(long tranId) {
			if ( this.tranId != tranId ) {
				throw new RuntimeException("Invalid state exception");
			}
			this.tranId = 0;
			notify();
		}

		public V getValue() {
			return value;
		}

		public long getNextVersion() {
			VersionContainer nextVer = this.nextVersion;
			if ( nextVer == null ) {
				return 0;
			}
			return nextVer.getValue();
		}

		public long getRowVersion() {
			return commitID.getValue();
		}

	}
	
	private final AtomicLong nextRowId = new AtomicLong(0);

	private final ListQueue<V> listQueue = new ListQueue<V>();

	private final List<PrimaryKey<?, V>> primaryKeysProvider;

	private final List<String> uniqueKeys = new ArrayList<String>();

	private final List<UniqueLock> uniqueLocks = new ArrayList<UniqueLock>();

	public DbList(List<PrimaryKey<?, V>> primaryKeysProvider) {
		this.primaryKeysProvider = primaryKeysProvider;
		for (PrimaryKey<?, V> primaryKey : primaryKeysProvider) {
			System.out.println("Primary keys : " + primaryKey.getKeyName());
			UniqueLock uniqueLock = new UniqueLock();
			uniqueLocks.add(uniqueLock);
			uniqueKeys.add(primaryKey.getKeyName());
		}
	}
	
	ListQueue<V> getListQueue() {
		return listQueue;
	}
	
	public RecordUniqueLockInfo primaryKeyCheck(V value, Long tranId) throws DuplicateKeysExistsException {
		List<Object> keyValues = new ArrayList<Object>();
		for (PrimaryKey<?, V> primaryKey : primaryKeysProvider) {
			Object keyValue = primaryKey.getKeyValue(value);
			keyValues.add(keyValue);		
		}
		RecordUniqueLockInfo recordUniqueLockInfo = UniqueLock.lockForUniqueKey(tranId, uniqueKeys, keyValues, uniqueLocks);
		return recordUniqueLockInfo;
		
	}
	
	long nextRowId() {
		return nextRowId.incrementAndGet();
	}

	public UpdatableCursor<V> createUpdatableCursor(long tranId, VersionContainer snapshotVersion, Copy<V> copier) {
		return createUpdatableCursor(null,tranId, snapshotVersion, copier);
	}

	public UpdatableCursor<V> createUpdatableCursor(RowFilter<V> rowFilter, long tranId, VersionContainer snapshotVersion, Copy<V> copier) {
		return new UpdatableCursor<V>(this, rowFilter, tranId, snapshotVersion, copier);
	}

	public ReadCursor<V> createReadCursor(long snapshotVersion, Copy<V> copier) {
		return createReadCursor(null, snapshotVersion, copier);
	}

	public ReadCursor<V> createReadCursor(RowFilter<V> rowFilter, long snapshotVersion, Copy<V> copier) {
		return new ReadCursor<V>(listQueue.getHead(), rowFilter, snapshotVersion, copier);
	}


	public void dump() {
		listQueue.print();
	}
	
}
