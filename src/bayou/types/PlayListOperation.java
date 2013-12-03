package bayou.types;

import java.util.UUID;

import bayou.entities.ProcessId;

import com.google.gson.Gson;

public class PlayListOperation {
	public enum OperationTypes {
		QUERY("query"),ADD("add"),EDIT("edit"),DELETE("delete"),CREATE("create"),RETIRE("retire");
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
	public ProcessId execServer;
	public Long execStamp;
	public long commitNumber;
	public boolean finalExecuted;
	public String url;
	public String id;
	public PlayListOperation() { 
		id = UUID.randomUUID().toString();
		commitNumber = -1;
		finalExecuted = false;
	}
	public PlayListOperation(PlayListOperation copy) { 
		this.op = copy.op;
		this.name = copy.name; 
		this.execServer = copy.execServer; 
		this.execStamp = copy.execStamp; 
		this.url = copy.url; 
		this.id = copy.id;
		this.commitNumber = copy.commitNumber;
		this.finalExecuted = copy.finalExecuted;
	}
	public void operate(PlayList playList) { 
		if (op.equalsIgnoreCase(OperationTypes.ADD.value())) {
			playList.addSong(name, url);
		} else if (op.equalsIgnoreCase(OperationTypes.DELETE.value())){
			playList.removeSong(name);
		} else if (op.equalsIgnoreCase(OperationTypes.EDIT.value())) {
			playList.editSong(name, url);
		} else if(op.equalsIgnoreCase(OperationTypes.QUERY.value())) {
			System.out.println(playList.getSong(name));
	    } else if (op.equalsIgnoreCase(OperationTypes.CREATE.value())) {
			
	    } else { 
			
		}
		
	}
	//TODO: needs to be changed to handle CREATE & RETIRE ?
	public boolean isWriteOp() { 
		if (!op.equalsIgnoreCase(OperationTypes.QUERY.value())) {
			return true;
		}
		return false;
	}
	public boolean isCreateOp() {
		if(op.equalsIgnoreCase(OperationTypes.CREATE.value())) {
			return true;
		}
		return false;
	}
	public boolean isRetireOp() {
		if(op.equalsIgnoreCase(OperationTypes.RETIRE.value())) {
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
