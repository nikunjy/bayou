package bayou.types;
class Song { 
	public String name; 
	public String url;
	public Song(String name, String url) { 
		this.name = name; 
		this.url = url;
	}
	public String toString() { 
		String ret = "";
		ret+= this.name +" "+ this.url;
		return ret;
	}
}