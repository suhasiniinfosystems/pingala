package collection.db;

import java.util.ArrayList;
import java.util.List;

public class RecordUniqueLockInfo {

	private final List<UniqueLock> uniqueLockList = new ArrayList<UniqueLock>();
	private final List<Object> valueList = new ArrayList<Object>();
	
	public List<UniqueLock> getUniqueLockList() {
		return uniqueLockList;
	}
	
	public List<Object> getValueList() {
		return valueList;
	}
	
}
