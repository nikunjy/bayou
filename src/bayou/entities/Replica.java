package bayou.entities;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bayou.types.PlayList;
import bayou.types.PlayListOperation;


public class Replica extends Process {
	List<ProcessId> replicas;
	PlayList playList;
	List<PlayListOperation> ops;
	ProcessId primary;
	PrintWriter writer;
	int entropyThreshold = 1;
	Map<ProcessId, Long> versionVector;
	public Replica(Env env, ProcessId me, ProcessId primary) {
		this.env = env;
		this.me = me;
		this.primary = primary;
		replicas = new ArrayList<ProcessId>();
		ops = new ArrayList<PlayListOperation>();
		try {
			String name = "";
			String [] names = this.me.toString().split(":");
			for (int i = 0; i < names.length; i++) { 
				name += names[i];
			}
			writer = new PrintWriter(name+".txt", "UTF-8");
		} catch (Exception e) { 
			System.out.println(e);
		}
		playList = new PlayList();
		versionVector = new HashMap<ProcessId,Long>();
		env.addProc(me, this);
	}
	public void setReplicas(List<ProcessId> replicas) {
		 this.replicas = replicas;
	}
	@Override
	void body() {
		long timeStamp = 0; 
		for (;;) {
			BayouMessage msg = getNextMessage();
			if (msg instanceof RequestMessage) {
				RequestMessage message = (RequestMessage)msg;
				String cmd = message.op;
				PlayListOperation operation = PlayListOperation.getOperation(cmd);
				operation.id = this.me+":"+operation.id;
				operation.execStamp = timeStamp; 
				ops.add(operation);
				operation.operate(playList);
				if (ops.size() % entropyThreshold == 0) { 
					for (ProcessId replica : replicas) {
						if (this.me.equals(replica)) 
							continue;
						List<PlayListOperation> tempOps = new ArrayList<PlayListOperation>(ops);
						AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, tempOps);
					}
				}
				System.out.println(this.me +" executed "+ operation.serialize());
				timeStamp++;
			} else if (msg instanceof EntropyInitMessage) { 
				EntropyRequestMessage message = new EntropyRequestMessage(); 
				message.versionVector = versionVector;
				message.src = this.me; 
				message.dest = ((EntropyInitMessage)msg).src;
				sendMessage(message.dest, message);
			} else if ( msg instanceof EntropyResponseMessage) { 
				EntropyResponseMessage message = (EntropyResponseMessage)msg;
				ops.add(message.op);
				System.out.println(this.me +" received "+ message.op.serialize());
				versionVector.put(message.op.execServer, message.op.execStamp);
			}
		}
		
	}
}
