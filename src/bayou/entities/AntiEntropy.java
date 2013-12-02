package bayou.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import bayou.types.PlayListOperation;

public class AntiEntropy extends Process {
	ProcessId receiver;
	ProcessId senderReplica;
	public List<PlayListOperation> ops;
	public List<PlayListOperation> commitedOps;
	public long commitSeq;
	public AntiEntropy(Env env, ProcessId me, ProcessId senderReplica, ProcessId receiver, List<PlayListOperation> ops, List<PlayListOperation> commitedOps, 
			long commitSeq) {
		this.env = env;
		this.me = me;
		this.receiver = receiver;
		this.senderReplica = senderReplica;
		this.ops = new ArrayList<PlayListOperation>(); 
		this.commitedOps = new ArrayList<PlayListOperation>();
		for (PlayListOperation op : ops) { 
			this.ops.add(new PlayListOperation(op));
		}
		for (PlayListOperation op : commitedOps) { 
			this.commitedOps.add(new PlayListOperation(op));
		}
		this.commitSeq = commitSeq;
		env.addProc(me, this);
	}
	@Override
	void body() {
		EntropyInitMessage initMessage = new EntropyInitMessage(); 
		initMessage.senderReplica = senderReplica;
		initMessage.src = this.me; 
		initMessage.dest = receiver;
		//System.out.println("Sending anti entropy from "+this.me+" "+receiver);
		sendMessage(receiver, initMessage);
		
		BayouMessage msg = getNextMessage();
		EntropyRequestMessage message = (EntropyRequestMessage)msg;
		Map<ProcessId, Long> versionVector = message.versionVector;
		if (msg instanceof EntropyRequestMessage) {
			EntropyRequestMessage entropyMessage = ((EntropyRequestMessage) msg);
			System.out.println("Doing anti entropy "+this.me+" with "+msg.src + " "+entropyMessage.commitSeq +" "+this.commitSeq);
			for ( PlayListOperation op : ops) { 
				System.out.println(this.me+" "+op.serialize());
			}
			if (entropyMessage.commitSeq < this.commitSeq) { 
				Map<Long, PlayListOperation> sortedCommits = new TreeMap<Long, PlayListOperation>();
				for (PlayListOperation op : commitedOps ) { 
					if (op.commitNumber != -1) { 
						sortedCommits.put(op.commitNumber, op);
					}
					else {
						System.out.println(senderReplica.toString()+": Committed op with commitNumber = -1 found, something is wrong...");
					}
				}
				for ( Long key : sortedCommits.keySet()) { 
					if ( key > entropyMessage.commitSeq) {
						PlayListOperation op = sortedCommits.get(key);
						//if ( op.execStamp <=  versionVector.get(op.execServer)) {
							CommitResponseMessage commitMessage = new CommitResponseMessage(sortedCommits.get(key));
							commitMessage.src = this.me; 
							commitMessage.dest = msg.src;
							commitMessage.commitNumber = key;
							sendMessage(msg.src, commitMessage);
						/*} else  {
							EntropyResponseMessage resp = new EntropyResponseMessage(); 
							resp.dest = message.src;
							resp.src = this.me;
							resp.setOp(op);
							sendMessage(message.src,resp);
						}*/
					}
				}	
			}
			for ( PlayListOperation op : ops) { 
				if (op.isWriteOp()) {
					
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
