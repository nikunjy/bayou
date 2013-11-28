package bayou.entities;
import java.util.*;

 public class Queue<T> {
	LinkedList<T> ll = new LinkedList<T>();
	public synchronized void enqueue(T obj){
		ll.add(obj);
		notify();
	}
	public synchronized T bdequeue(){
		while (ll.size() == 0) {
			try { wait(); } catch (InterruptedException e) {}
		}
		return ll.removeFirst();
	}
	public synchronized T bdequeue(long timeOut){
		try { wait(timeOut); } catch (InterruptedException e) {}
		if (ll.size() == 0) { 
			return null;
		}
		return ll.removeFirst();
	}
}
