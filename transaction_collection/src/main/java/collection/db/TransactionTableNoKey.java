package collection.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

public class TransactionTableNoKey<V> {

	private TransactionalTable<V> transactionalTable;
	
	public TransactionTableNoKey(Copy<V> copier) {
		this(copier, null);
	}

	public TransactionTableNoKey(Copy<V> copier, Collection<V> coll) {
		List<PrimaryKey<?, V>> primaryKeysProvider = new ArrayList<PrimaryKey<?, V>>();
		try {
			TransactionalTable<V> transactionalTable = new TransactionalTable<V>(copier, coll, primaryKeysProvider);
			this.transactionalTable = transactionalTable;
		} catch (DuplicateKeysExistsException e) {
			// TODO: handle exception
		}
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
