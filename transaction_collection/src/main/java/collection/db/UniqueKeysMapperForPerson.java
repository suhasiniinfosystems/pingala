package collection.db;

import collection.db.test.Person;

public class UniqueKeysMapperForPerson implements UniqueKeysMapper {

	public String[] getKeyNames() {
		return new String[] { "name" };
	}
	
	public Object[] getKeyValues(Object record) {
		Person person = (Person)record;
		return new Object[] { person.getName() };
	}
	
}
