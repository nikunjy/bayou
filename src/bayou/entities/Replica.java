package bayou.entities;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import bayou.types.PlayList;
import bayou.types.PlayListOperation;


public class Replica extends Process {
	List<ProcessId> replicas;
	ProcessId primary;
	PlayList playList;
	PlayList commitedPlayList;
	List<PlayListOperation> ops;
	List<PlayListOperation> commitedOps;
	Map<String, PlayListOperation> uniqueCopies;
	PrintWriter writer;
	int entropyThreshold = 1;
	boolean isPrimary;
	long commitSeq;
	Map<ProcessId, Long> versionVector;
	public Replica(Env env, ProcessId me, ProcessId primary) {
		this.env = env;
		this.me = me;
		this.primary = primary;
		replicas = new ArrayList<ProcessId>();
		ops = new ArrayList<PlayListOperation>();
		commitedOps = new ArrayList<PlayListOperation>();
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
		commitedPlayList = new PlayList();
		isPrimary = false;
		commitSeq = -1;
		if (me.equals(primary)) { 
			isPrimary = true;
			this.commitSeq = 0;
		}
		versionVector = new HashMap<ProcessId,Long>();
		uniqueCopies = new HashMap<String, PlayListOperation>();
		
		env.addProc(me, this);
	}
	public void setReplicas(List<ProcessId> replicas) {
		this.replicas = replicas;
	}
	@Override
	void body() {
		long timeStamp = 0; 
		if (isPrimary) {
			writer.println("I am primary");
			for (;;) {
				BayouMessage msg = getNextMessage();
				if (msg instanceof CommitRequestMessage) { 
					CommitRequestMessage req = (CommitRequestMessage)msg;
					writer.println("Received commit request from "+req.src+" with commit number "+req.senderCommitNumber);
					req.op.commitNumber = commitSeq; 
					ops.add(new PlayListOperation(req.op));
					ProcessId receiver = msg.src; 
					writer.println("Primary replica assigning commit number to "+ req.op.serialize()+" assigned "+commitSeq +" received from "+ receiver);
					commitSeq++;
					for (int i = 0; i < commitSeq ; i ++) {
						CommitResponseMessage commitedMessage = new CommitResponseMessage(new PlayListOperation(ops.get(i)));
						commitedMessage.commitNumber = ops.get(i).commitNumber;
						commitedMessage.src = this.me; 
						commitedMessage.dest = receiver;
						writer.println("Sending commited message "+ ops.get(i).serialize()+" to  "+ commitedMessage.dest);
						sendMessage(receiver, commitedMessage);
					}
					writer.flush();
				}
			}
		} else {
			for (;;) {
				BayouMessage msg = getNextMessage();
				if (msg instanceof RequestMessage) {
					RequestMessage message = (RequestMessage)msg;
					String cmd = message.op;
					PlayListOperation operation = PlayListOperation.getOperation(cmd);
					operation.id = this.me+":"+operation.id;
					operation.execStamp = timeStamp;
					operation.execServer = this.me;
					ops.add(operation);
					uniqueCopies.put(operation.id, operation);
					operation.operate(playList);
					if (ops.size() % entropyThreshold == 0) {
						for (PlayListOperation tempOp : ops) { 
							if (tempOp.commitNumber != -1) 
								continue;
							CommitRequestMessage commitReq = new CommitRequestMessage(new PlayListOperation(tempOp)); 
							commitReq.src = this.me; 
							commitReq.dest = primary; 
							commitReq.senderCommitNumber = this.commitSeq;
							sendMessage(primary, commitReq);
						}
						for (ProcessId replica : replicas) {
							if (this.me.equals(replica)) 
								continue;
							AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
									this.commitSeq);
						}
					}
					System.out.println(this.me +" executed "+ operation.serialize());
					timeStamp++;
				} else if (msg instanceof EntropyInitMessage) {
					EntropyRequestMessage message = new EntropyRequestMessage(); 
					message.versionVector = versionVector;
					message.commitSeq = this.commitSeq;
					message.src = this.me; 
					message.dest = ((EntropyInitMessage)msg).src;
					sendMessage(message.dest, message);
				} else if ( msg instanceof EntropyResponseMessage) { 
					EntropyResponseMessage message = (EntropyResponseMessage)msg;
					if (!uniqueCopies.containsKey(message.op.id)) {
						ops.add(message.op);
						writer.println("Receiving entropy response message from "+message.src +" "+message.op.serialize());
						writer.flush();
						this.commitSeq = (this.commitSeq < message.op.commitNumber) ? message.op.commitNumber : this.commitSeq;
						uniqueCopies.put(message.op.id, message.op);
					}
					versionVector.put(message.op.execServer, message.op.execStamp);
				} else if ( msg instanceof CommitResponseMessage) { 
					CommitResponseMessage commitResponse = (CommitResponseMessage)msg;
					writer.println("Receiving commited message from "+commitResponse.src +" "+ commitResponse.commitNumber+" "+commitResponse.op.serialize());
					this.commitSeq = (this.commitSeq < commitResponse.commitNumber) ? commitResponse.commitNumber : this.commitSeq;
					writer.println(this.me+" setting commit sequence number "+commitSeq);					
					Iterator<PlayListOperation> it = ops.iterator();
					PlayListOperation commitedOperation = commitResponse.op;
					while (it.hasNext()) { 
						PlayListOperation op = it.next(); 
						if (op.id.equals(commitedOperation.id)) { 
							it.remove();
						}
					}
					boolean found = false; 
					for (PlayListOperation op : commitedOps) {
						if (op.id.equals(commitedOperation.id)) { 
							found = true; 
							break;
						}
					}
					if (!found) { 
						commitedOps.add(commitedOperation);
						commitedOperation.operate(commitedPlayList);
						writer.println("Executing commited operation"+commitedOperation.serialize());
					}
					uniqueCopies.put(commitedOperation.id, commitedOperation);
					writer.flush();
				} else if (msg instanceof UserEntropyInitMessage) { 
					UserEntropyInitMessage initRequest = (UserEntropyInitMessage)msg;
					ProcessId replica = initRequest.receiver;
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
							this.commitSeq);
					
				} else if (msg instanceof PrintLogRequestMessage) { 
					PrintLogRequestMessage logRequest = (PrintLogRequestMessage)msg;
					if (logRequest.showCommitSeq) {
						writer.println("###########################");
						writer.println("Commit Seq " + this.commitSeq);
						writer.println("###########################");
					}
					if (logRequest.showCommitedOps) {
						writer.println("Showing commited ops");
						writer.println("###########################");
						for (PlayListOperation op : commitedOps){ 
							writer.println(op.serialize());
						}
						writer.println("###########################");
					}
					if (logRequest.showTentativeWrites) { 
						writer.println("Showing Tentative writes");
						writer.println("###########################");
						for (PlayListOperation op : ops){ 
							if (op.commitNumber == -1) { 
								writer.println(op.serialize());
							}
						}
						writer.println("###########################");
					}
					writer.println("PlayList");
					writer.println("###########################");
					System.out.println(playList.toString());
					writer.println("Commited PlayList");
					writer.println("###########################");
					System.out.println(commitedPlayList.toString());
					writer.flush();
				}
			}
		}
	}
}
