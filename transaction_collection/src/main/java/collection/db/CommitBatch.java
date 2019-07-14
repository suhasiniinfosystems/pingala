package collection.db;

import java.util.List;

import collection.db.DbList.VersionContainer;

@SuppressWarnings("rawtypes")
class CommitBatch {

	private final List<CommitList> commitListCollection;
	
	public CommitBatch(List<CommitList> commitListCollection) {
		this.commitListCollection = commitListCollection;
	}

	public void prepareToAdd(VersionContainer commitID) {
		for (CommitList commitList : commitListCollection) {
			commitList.prepareToAdd(commitID);
		}
	}
	
	public void add() {
		for (CommitList commitList : commitListCollection) {
			commitList.add();
		}
	}
	
	
}
