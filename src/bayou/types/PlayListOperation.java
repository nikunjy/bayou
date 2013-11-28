package bayou.types;

import java.util.UUID;

import com.google.gson.Gson;

public class PlayListOperation {
	public enum OperationTypes {
		QUERY("query"),ADD("add"),EDIT("edit"),DELETE("delete");
		public String message;
		public String value() { 
			return message;
		}
		OperationTypes(String message) {
			this.message = message;
		}
	}
	public String op;
	public String name; 
	public String url;
	public String id;
	public PlayListOperation() { 
		id = UUID.randomUUID().toString();
	}
	public void operate(PlayList playList) { 
		if (op.equalsIgnoreCase(OperationTypes.ADD.value())) {
			playList.addSong(name, url);
		} else if (op.equalsIgnoreCase(OperationTypes.DELETE.value())){
			playList.removeSong(name);
		} else if (op.equalsIgnoreCase(OperationTypes.EDIT.value())) {
			playList.editSong(name, url);
		} else { 
			
		}
	}
	public boolean isWriteOp() { 
		if (!op.equalsIgnoreCase(OperationTypes.QUERY.value())) {
			return true;
		}
		return false;
	}
	public static PlayListOperation getOperation(String msg) { 
		Gson gson = new Gson(); 
		return gson.fromJson(msg, PlayListOperation.class);
	}
	public String serialize() { 
		Gson gson = new Gson(); 
		return gson.toJson(this);
	}
}
