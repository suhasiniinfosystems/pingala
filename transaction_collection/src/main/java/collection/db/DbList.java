package collection.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import collection.db.DbList.UpdatableCursor.UpdateItem;

public class DbList<V extends Copy<V>> {
	
	public static class ReadCursor<V extends Copy<V>> {

		private ItemNode<V> mNode;
		private long snapshotVersion;
		
		public ReadCursor(ItemNode<V> head, long snapshotVersion) {
			this.mNode = head;
			this.snapshotVersion = snapshotVersion;
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
					this.mNode = node;
					return true;
				} else if ( version < snapshotVersion ) {
					long nextVersion = item.getNextVersion();
					if ( nextVersion == 0 ) {
						this.mNode = node;
						return true;
					} else if ( nextVersion <= snapshotVersion ) {
						continue;
					} else {
						this.mNode = node;
						return true;
					}
				}
			}
		}

		public V get() {
			V value = mNode.getItem().getValue();
			V copy = value.copy();
			return copy;
		}
		
	}

	public static class UpdatableCursor<V extends Copy<V>> {

		static public class UpdateItem<V> {
			private Item<V> currentItem;
			private V nextValue;
			public UpdateItem(Item<V> currentItem, V nextValue) {
				this.currentItem = currentItem;
				this.nextValue = nextValue;
			}
			public Item<V> getCurrentItem() {
				return currentItem;
			}
			public V getNextValue() {
				return nextValue;
			}
		}


		private final DbList<V> dbList;
		private ItemNode<V> mNode;
		private V copy;
		
		
		private List<UpdateItem<V>> updates = new ArrayList<UpdateItem<V>>();
		//private List<UpdateItem<V>> adds = new ArrayList<UpdateItem<V>>();
		
		private long snapshotVersion;
		private final long tranId;
		private final RowFilter<V> rowFilter;
		
		public UpdatableCursor(DbList<V> dbList, ItemNode<V> head, RowFilter<V> rowFilter) {
			this.dbList = dbList;
			this.mNode = head;
			this.tranId = dbList.nextTranId();
			this.snapshotVersion = dbList.getLatestVersion();
			this.rowFilter = rowFilter;
		}

		/*
		public void add(V value) {
			long rowId = dbList.nextRowId();
			UpdateItem<V> updateItem = new UpdateItem<V>(rowId, value);
			adds.add(updateItem);
		}
		*/

		public void printNode() {
			Item<V> item = mNode.getItem();
			V value = get();
			System.out.println(" rowId : " + item.getRowId() + ", commitId : " + item.getCommitId() + ", nextVersionId : " + item.getNextVersion() + " [ " + value.toString() + " ]");
		}
		
		public V get() {
			V value = mNode.getItem().getValue();
			copy = value.copy();
			return copy;
		}

		public void update(V updatedValue) {
			ItemNode<V> node = mNode;
			if ( node == null || mNode.getItem() == null ) {
				throw new RuntimeException("Invalid update request");
			} 	
			Item<V> item = mNode.getItem();
			UpdateItem<V> updateItem = new UpdateItem<V>(item, updatedValue);
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
				if ( version > snapshotVersion ) {
					return false;
				} else if ( version <= snapshotVersion ) {
					item.lock();
					long nextVersion = item.getNextVersion();
					if ( nextVersion == 0 ) {
						System.out.println(Thread.currentThread().getName() + " - nextVer : " + item.getNextVersion());
						V value = item.getValue();
						if ( rowFilter == null || rowFilter.accept(value)) {
							item.lockForUpdate(tranId);
							// Recheck
							nextVersion = item.getNextVersion();
							if ( nextVersion != 0  ) {
								// There is a valid next version
								// make that as the snapshot version and skip this row
								snapshotVersion = nextVersion;
								item.unlockNoUpdate(tranId);
								// Explicit unlock is not needed now
								//item.unlock();
								continue;
							}
							
							this.mNode = node;
							System.out.println(Thread.currentThread().getName() + " - Matched item.rowId : " + item.getRowId() + ", commitID : " + item.getCommitId() + ", nextVer : " + item.getNextVersion());
							item.unlock();
							return true;						
						} else {
							item.unlock();
							continue;
						}
					} else {
						// There is a valid next version
						// make that as the snapshot version and skip this row
						snapshotVersion = nextVersion;
						item.unlock();
						continue;
					}
				}
			}
		}

		public void commit() {
			CommitID commitID = dbList.commit(null, updates);
			for (UpdateItem<V> updateItem : updates) {
				Item<V> currentItem = updateItem.getCurrentItem();
				currentItem.unlockAfterUpdate(tranId, commitID);
			}
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
	
	public static class CommitID {

		private volatile long commitId;

		public long getCommitId() {
			return commitId;
		}

		public void setCommitId(long commitId) {
			this.commitId = commitId;
		}
		
		@Override
		public String toString() {
			return String.valueOf(commitId);
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

		private final ReentrantLock lock = new ReentrantLock(true);
		private final Condition condition = lock.newCondition();
		
		private final long rowId;
		private final V value;
		private final CommitID commitID;

		private volatile long tranId; 

		private volatile CommitID nextVersion;

		public Item(long rowId, V value, CommitID commitID) {
			this.rowId = rowId;
			this.value = value;
			this.commitID = commitID;
		}
		
		public long getRowId() {
			return rowId;
		}

		public long getCommitId() {
			return commitID.getCommitId();
		}

		public void lock() {
			lock.lock();
		}	

		public void unlock() {
			lock.unlock();
		}	

		public void lockForUpdate(long tranId) {
			//lock.lock();
			try {
				if ( this.tranId == tranId ) {
					throw new RuntimeException("Invalid state exception");
				}	
				while ( this.tranId != 0 && this.tranId != tranId ) {
					try {
						condition.await();
					} catch (Exception e) {
					}
				}
				this.tranId = tranId;
			} finally {
			}
		}

		public void unlockNoUpdate(long tranId) {
			try {
				if ( this.tranId != tranId ) {
					throw new RuntimeException("Invalid state exception");
				}
				this.tranId = 0;
				System.out.println(Thread.currentThread().getName() + " - Unlock before item.rowId : " + rowId + ", commitID : " + commitID + ", nextVer : " + nextVersion);
				System.out.println(Thread.currentThread().getName() + " - Unlock after item.rowId : " + rowId + ", commitID : " + commitID + ", nextVer : " + nextVersion);
				condition.signal();
			} finally {
			}
		}

		public void unlockAfterUpdate(long tranId, CommitID nextCommitID) {
			lock.lock();
			try {
				if ( this.tranId != tranId ) {
					throw new RuntimeException("Invalid state exception");
				}
				this.tranId = 0;
				System.out.println(Thread.currentThread().getName() + " - Unlock before item.rowId : " + rowId + ", commitID : " + commitID + ", nextVer : " + nextVersion);
				this.nextVersion = nextCommitID;
				System.out.println(Thread.currentThread().getName() + " - Unlock after item.rowId : " + rowId + ", commitID : " + commitID + ", nextVer : " + nextVersion);
				condition.signal();
			} finally {
				lock.unlock();
			}
		}
		
		public V getValue() {
			return value;
		}

		public long getNextVersion() {
			CommitID nextVer = this.nextVersion;
			if ( nextVer == null ) {
				return 0;
			}
			return nextVer.getCommitId();
		}

		public long getRowVersion() {
			return commitID.getCommitId();
		}

	}
	
	private final AtomicLong nextRowId = new AtomicLong(0);
	private final AtomicLong nextTranId = new AtomicLong(0);
	private final AtomicLong commitRunningNumber = new AtomicLong(0L);

	private final ListQueue<V> listQueue = new ListQueue<V>();
	private final ReentrantLock commitLock = new ReentrantLock(true);
	private final ConcurrentLinkedDeque<CommitID> versionDeque = new ConcurrentLinkedDeque<CommitID>();

	public long getLatestVersion() {
		return versionDeque.getLast().getCommitId();
	}

	public DbList(Collection<V> coll) {
		commit(coll, null);
	}
	
	public long nextRowId() {
		return nextRowId.incrementAndGet();
	}

	public long nextTranId() {
		return nextTranId.incrementAndGet();
	}

	public UpdatableCursor<V> createUpdatableCursor() {
		return createUpdatableCursor(null);
	}

	public UpdatableCursor<V> createUpdatableCursor(RowFilter<V> rowFilter) {
		return new UpdatableCursor<V>(this, listQueue.getHead(), rowFilter);
	}
	
	public ReadCursor<V> createReadCursor() {
		long snapshotVersion = getLatestVersion();
		return new ReadCursor<V>(listQueue.getHead(), snapshotVersion);
	}

	void updateToQueue(List<UpdateItem<V>> updateItems, CommitID commitID, ListQueue<V> newListQueue) {
		for (UpdateItem<V> updateItem : updateItems) {
			Item<V> item = new Item<V>(updateItem.getCurrentItem().getRowId(), updateItem.getNextValue(), commitID);
			newListQueue.add(item);
		}
	}

	void addToQueue(Collection<V> values, CommitID commitID, ListQueue<V> newListQueue) {
		for (V value : values) {
			long rowId = nextRowId();
			Item<V> item = new Item<V>(rowId, value, commitID);
			newListQueue.add(item);
		}
	}

	public CommitID commit(Collection<V> addItems, List<UpdateItem<V>> updateItems) {
		ListQueue<V> newListQueue = new ListQueue<V>();
		CommitID commitID = new CommitID();
		if ( updateItems != null && updateItems.size() > 0 ) {
			updateToQueue(updateItems, commitID, newListQueue);
		}
		if ( addItems != null && addItems.size() > 0 ) {
			addToQueue(addItems, commitID, newListQueue);
		}
		commitLock.lock();
		try {
			listQueue.addNodes(newListQueue);
			long commitId = commitRunningNumber.incrementAndGet();
			commitID.setCommitId(commitId);
			versionDeque.add(commitID);
		} finally {
			commitLock.unlock();
		}
		return commitID;
	}

	public void dump() {
		listQueue.print();
	}
	
}
