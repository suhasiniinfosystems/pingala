package collection.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import collection.db.DbList.Item;
import collection.db.DbList.VersionContainer;
import collection.db.DbList.UpdatableCursor.UpdateItem;

public class Util<V extends Copy<V>> {

	public void commit(Collection<V> adds, List<UpdateItem<V>> updatedItems, long tranId, DbList<V> dbList, CommitManager commitManager) {
		CommitList commitList = new  CommitList(dbList, adds, updatedItems);
		List<CommitList> commitListColl = new ArrayList<CommitList>();
		CommitBatch commitBatch = new CommitBatch(commitListColl);
		VersionContainer commitId= commitManager.commit(commitBatch);
		for (UpdateItem<V> updateItem : updatedItems) {
			Item<V> currentItem = updateItem.getCurrentItem();
			currentItem.unlockAfterUpdate(tranId, commitId);
		}
		
	}
	
}
