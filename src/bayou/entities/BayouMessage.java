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
class CommitRequestMessage extends BayouMessage { 
	public PlayListOperation op;
	public long senderCommitNumber;
	public CommitRequestMessage(PlayListOperation op) { 
		super();
		this.op = op; 
	}
}
class PrintLogRequestMessage extends BayouMessage { 
	public boolean showCommitSeq; 
	public boolean showCommitedOps; 
	public boolean showTentativeWrites;
	public PrintLogRequestMessage(int logLevel) { 
		super();
		switch(logLevel) { 
		case 0:
			showCommitSeq = true; 
			break;
		case 1: 
			showCommitedOps = true;
			break;
		case 2:
			showTentativeWrites = true;
			break;
		case 3: 
			showCommitedOps = true; 
			showTentativeWrites = true;
			break;
		}
	}
}
class UserEntropyInitMessage extends BayouMessage { 
	public ProcessId receiver;  
	public UserEntropyInitMessage(ProcessId receiver) {
		super();
		this.receiver = receiver;
		
	}
}
class CommitResponseMessage extends BayouMessage { 
	public PlayListOperation op;
	public long commitNumber;
	public CommitResponseMessage(PlayListOperation op) { 
		super();
		this.op = op; 
	}
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
	public long commitSeq;
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