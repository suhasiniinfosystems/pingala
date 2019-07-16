package collection.db.test;

import collection.db.PrimaryKey;

public class PersonKeyName implements PrimaryKey<String, Person> {

	public String getKeyName() {
		return "name";
	}

	public String getKeyValue(Person person) {
		return person.getName();
	}
	
}
