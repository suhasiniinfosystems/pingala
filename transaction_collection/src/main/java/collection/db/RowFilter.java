package collection.db;

public interface RowFilter<V> {

	public boolean accept(V value);
	
}
