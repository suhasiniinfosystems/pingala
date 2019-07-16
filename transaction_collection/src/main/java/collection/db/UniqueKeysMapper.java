package collection.db;

public interface UniqueKeysMapper {

	public String[] getKeyNames();
	
	public Object[] getKeyValues(Object record);
	
}
