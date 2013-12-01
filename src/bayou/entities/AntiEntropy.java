package bayou.entities;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import bayou.types.PlayListOperation;

public class AntiEntropy extends Process {
	ProcessId receiver;
	ProcessId senderReplica;
	public List<PlayListOperation> ops;
	public long commitSeq;
	public AntiEntropy(Env env, ProcessId me, ProcessId senderReplica, ProcessId receiver, List<PlayListOperation> ops, long commitSeq) {
		this.env = env;
		this.me = me;
		this.receiver = receiver;
		this.senderReplica = senderReplica;
		this.ops = ops;
		this.commitSeq = commitSeq;
		env.addProc(me, this);
	}
	@Override
	void body() {
		EntropyInitMessage initMessage = new EntropyInitMessage(); 
		initMessage.senderReplica = senderReplica;
		initMessage.src = this.me; 
		initMessage.dest = receiver;
		sendMessage(receiver, initMessage);
		BayouMessage msg = getNextMessage();
		EntropyRequestMessage message = (EntropyRequestMessage)msg;
		Map<ProcessId, Long> versionVector = message.versionVector;
		if (msg instanceof EntropyRequestMessage) {
			EntropyRequestMessage entropyMessage = ((EntropyRequestMessage) msg);
			if (entropyMessage.commitSeq < this.commitSeq) { 
				Map<Long, PlayListOperation> commitedOps = new TreeMap<Long, PlayListOperation>();
				for (PlayListOperation op : ops) { 
					if (op.commitNumber != -1) { 
						commitedOps.put(op.commitNumber, op);
					}
				}
				for ( Long key : commitedOps.keySet()) { 
					if ( key > entropyMessage.commitSeq) {
						PlayListOperation op = commitedOps.get(key);
						if ( op.execStamp <=  versionVector.get(op.execServer)) {
							CommitResponseMessage commitMessage = new CommitResponseMessage(commitedOps.get(key));
							commitMessage.src = this.me; 
							commitMessage.dest = msg.src;
							sendMessage(msg.src, commitMessage);
						} else  {
							EntropyResponseMessage resp = new EntropyResponseMessage(); 
							resp.dest = message.src;
							resp.src = this.me;
							resp.setOp(op);
							sendMessage(message.src,resp);
						}
					}
				}	
			}
			for ( PlayListOperation op : ops) { 
				if (op.isWriteOp() && op.commitNumber == -1) {
					if (!versionVector.containsKey(op.execServer) || versionVector.get(op.execServer) < op.execStamp) { 
						EntropyResponseMessage resp = new EntropyResponseMessage(); 
						resp.dest = message.src;
						resp.src = this.me;
						resp.setOp(op);
						sendMessage(message.src,resp);
					}
				}
			}
		}
	}

}
