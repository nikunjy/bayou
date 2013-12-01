package bayou.types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
public class PlayList {
	List<Song> songs;
	public PlayList() { 
		songs = new ArrayList<Song>();
	}
	public List<Song> getSongs() { 
		return songs;
	}
	public void addSong(String name, String url) { 
		songs.add(new Song(name,url));
	}
	public void removeSong(String name) { 
		Iterator<Song> it = songs.iterator();
		while(it.hasNext()) { 
			Song song = it.next(); 
			if (song.name.equalsIgnoreCase(name))  {
				it.remove(); 
				break;
			}
		}
	}
	public void editSong(String name, String url) { 
		Iterator<Song> it = songs.iterator();
		while(it.hasNext()) { 
			Song song = it.next(); 
			if (song.name.equalsIgnoreCase(name))  {
				song.url = url;
				break;
			}
		}
	}
	public String toString() { 
		String ret = "";
		for (Song song : songs) { 
			ret += song.toString();
		}
		return ret;
	}
}
