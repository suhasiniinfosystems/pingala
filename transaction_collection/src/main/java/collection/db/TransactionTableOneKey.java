package collection.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

public class TransactionTableOneKey<V, K> {

	private TransactionalTable<V> transactionalTable;
	
	public TransactionTableOneKey(Copy<V> copier, PrimaryKey<K, V> pkOne) throws DuplicateKeysExistsException {
		this(copier, null, pkOne);
	}

	public TransactionTableOneKey(Copy<V> copier, Collection<V> coll, PrimaryKey<K, V> pkOne) throws DuplicateKeysExistsException {
		List<PrimaryKey<?, V>> keyProviders = new ArrayList<PrimaryKey<?, V>>();
		keyProviders.add(pkOne);

		TransactionalTable<V> transactionalTable = new TransactionalTable<V>(copier, coll, keyProviders);
		this.transactionalTable = transactionalTable;
	}

	public ReadWriteCursor<V> createReadWriteCursor() {
		return transactionalTable.createReadWriteCursor();
	}

	public ReadWriteCursor<V> createReadWriteCursor(RowFilter<V> rowFilter) {
		return transactionalTable.createReadWriteCursor(rowFilter);
	}

	public SnapshotCursor<V> createSnapshotCursor() {
		return transactionalTable.createSnapshotCursor();
	}

	public SnapshotCursor<V> createSnapshotCursor(RowFilter<V> rowFilter) {
		return transactionalTable.createSnapshotCursor(rowFilter);
	}
	
	public void dump() {
		transactionalTable.dump();
	}

	
}
