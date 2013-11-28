package bayou.entities;

import java.util.ArrayList;
import java.util.List;

import bayou.types.PlayListOperation;

public class BayouMessage {
	ProcessId src; 
	ProcessId dest; 
}
class RequestMessage extends BayouMessage {
	public String op;
	public RequestMessage(String op) { 
		super();
		this.op = op;
	}
}
class ResponseMessage extends BayouMessage {
	String response; 
	public ResponseMessage() { 
		super();
	}	
}
class EntropyInitMessage extends BayouMessage { 
	public List<PlayListOperation> ops;
	public EntropyInitMessage() { 
		super();
		ops = new ArrayList<PlayListOperation>();
	}
}
class EntropyResponseMessage extends BayouMessage { 
	public List<PlayListOperation> ops;
	public EntropyResponseMessage() { 
		super(); 
		ops = new ArrayList<PlayListOperation>();
	}
}