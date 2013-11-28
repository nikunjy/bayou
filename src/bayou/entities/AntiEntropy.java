package bayou.entities;

import java.util.List;
import java.util.Map;

import bayou.types.PlayListOperation;

public class AntiEntropy extends Process {
	ProcessId receiver;
	ProcessId senderReplica;
	public List<PlayListOperation> ops;
	public AntiEntropy(Env env, ProcessId me, ProcessId senderReplica, ProcessId receiver, List<PlayListOperation> ops) {
		this.env = env;
		this.me = me;
		this.receiver = receiver;
		this.senderReplica = senderReplica;
		this.ops = ops;
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
		if (msg instanceof EntropyRequestMessage) { 
			EntropyRequestMessage message = (EntropyRequestMessage)msg;
			Map<ProcessId, Long> versionVector = message.versionVector;
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
