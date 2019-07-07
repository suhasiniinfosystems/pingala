package collection.db.test;

import collection.db.Copy;

public class Person implements Copy<Person> {

	private String name;
	private int age;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}

	public Person copy() {
		Person person = new Person();
		person.setName(name);
		person.setAge(age);
		return person;
	}
	
	public String toString() {
		return "name : " + name + ", age : " + String.valueOf(age);
	}
	
}
