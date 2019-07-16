package collection.db;

public class DuplicateKeysExistsException extends SqlException {

	public DuplicateKeysExistsException(String msg) {
		super(msg);
	}
}
