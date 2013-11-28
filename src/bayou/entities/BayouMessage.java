package bayou.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bayou.types.PlayListOperation;

public class BayouMessage {
	ProcessId src; 
	ProcessId dest; 
}
class RequestMessage extends BayouMessage {
	public String op;
	public RequestMessage(ProcessId src, ProcessId dest, String op) { 
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
class EntropyRequestMessage extends BayouMessage { 
	public Map<ProcessId, Long> versionVector;
	public EntropyRequestMessage() { 
		super();
		versionVector = new HashMap<ProcessId,Long>();
	}
}
class EntropyInitMessage extends BayouMessage {
	public ProcessId senderReplica;
	public EntropyInitMessage() { 
		super();
	}
}
class EntropyResponseMessage extends BayouMessage { 
	PlayListOperation op;
	public EntropyResponseMessage() { 
		super(); 
	}
	public void setOp(PlayListOperation cp) { 
		this.op = new PlayListOperation(cp);
	}
}