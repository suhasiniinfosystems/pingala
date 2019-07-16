package collection.db;

public interface PrimaryKey<K, V> {

	public String getKeyName();

	public K getKeyValue(V record);
	
}
