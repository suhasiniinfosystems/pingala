package collection.db;

import java.util.Collection;
import java.util.List;

import collection.db.DbList.Item;
import collection.db.DbList.ListQueue;
import collection.db.DbList.UpdatableCursor.UpdateItem;
import collection.db.DbList.VersionContainer;

class CommitList<V extends Copy<V>> {

	private final DbList<V> dbList;
	private final Collection<V> addItems;
	private final List<UpdateItem<V>> updateItems;
	private final ListQueue<V> newListQueue = new ListQueue<V>();
	
	public CommitList(DbList<V> dbList, Collection<V> addItems, List<UpdateItem<V>> updateItems) {
		this.dbList = dbList;
		this.addItems = addItems;
		this.updateItems = updateItems;
	}

	public void prepareToAdd(VersionContainer commitID) {
		if ( updateItems != null && updateItems.size() > 0 ) {
			updateToQueue(updateItems, commitID, newListQueue);
		}
		if ( addItems != null && addItems.size() > 0 ) {
			addToQueue(addItems, commitID, newListQueue);
		}
	}
	
	public void add() {
		ListQueue<V> listQueue = dbList.getListQueue();
		listQueue.addNodes(newListQueue);
	}
	
	public void addToQueue(VersionContainer commitID) {
		ListQueue<V> newListQueue = new ListQueue<V>();

		if ( updateItems != null && updateItems.size() > 0 ) {
			updateToQueue(updateItems, commitID, newListQueue);
		}
		if ( addItems != null && addItems.size() > 0 ) {
			addToQueue(addItems, commitID, newListQueue);
		}
	}
	
	private void updateToQueue(List<UpdateItem<V>> updateItems, VersionContainer commitID, ListQueue<V> newListQueue) {
		for (UpdateItem<V> updateItem : updateItems) {
			Item<V> item = new Item<V>(updateItem.getCurrentItem().getRowId(), updateItem.getNextValue(), commitID);
			newListQueue.add(item);
		}
	}

	private void addToQueue(Collection<V> values, VersionContainer commitID, ListQueue<V> newListQueue) {
		for (V value : values) {
			long rowId = dbList.nextRowId();
			Item<V> item = new Item<V>(rowId, value, commitID);
			newListQueue.add(item);
		}
	}

	
}
