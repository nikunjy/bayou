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
	List<ProcessId> connectedReplicas;
	ProcessId primary;
	PlayList playList;
	PlayList commitedPlayList;
	List<PlayListOperation> ops;
	List<PlayListOperation> commitedOps;
	Map<String, PlayListOperation> uniqueCopies;
	Map<String, PlayListOperation> uniqueCommits;
	PrintWriter writer;
	int entropyThreshold = 1;
	boolean isPrimary;
	long commitSeq;
	Map<ProcessId, Long> versionVector;
	Long timeStamp;
	boolean isActive;
	public Replica(Env env, ProcessId me, ProcessId primary) {
		this.env = env;
		this.me = me;
		this.primary = primary;
		//this.connectedReplicas = neigh;
		this.replicas = new ArrayList<ProcessId>();
		this.connectedReplicas = new ArrayList<ProcessId>();
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
		uniqueCommits = new HashMap<String, PlayListOperation>();
		this.timeStamp = (long)0;
		this.isActive = true;
		env.addProc(me, this);
	}
	public void setConnectedList(List<ProcessId> neigh) {
		for(ProcessId p: neigh)
			this.connectedReplicas.add(p);
	}
	public void setReplicas(List<ProcessId> replicas) {
		for(ProcessId p: replicas)
			this.replicas.add(p);
		
	}
	public void removeFromConnectedList(ProcessId p) {
		this.connectedReplicas.remove(p);
	}
	public void addToConnectedList(ProcessId p) {
		this.connectedReplicas.add(p);
	}
	public void setTimeStamp(long t) {
		timeStamp = (long)t;
	}
	@Override
	void body() {
		
		if (isPrimary) {
			writer.println("I am primary");
			writer.flush();
		} 
		if(this.replicas.size() == 0){
			System.out.println(this.me+": I am not aware of any replica..");
		}
		if(this.connectedReplicas.size() == 0){
			System.out.println(this.me+": I am not connected to any replica..");
		}
		while(isActive) {
			/*for (ProcessId replica : this.connectedReplicas) {
				System.out.println(this.me+": connected to "+replica.toString());
				System.out.println(this.me+": Size of connected replicas = "+connectedReplicas.size());
			}*/
			/*if(connectedReplicas.contains(primary)){
				System.out.println(this.me+": is connected to primary..");
			}
			else {
				System.out.println(this.me+": not connected to primary..");
			}*/
			BayouMessage msg = getNextMessageWait();
			/*if(msg == null ) {
				//System.out.println(this.me+": Going to perform anti-entropy due to lack of messages");
				try {
					sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				for (ProcessId replica : this.connectedReplicas) {
					if (this.me.equals(replica)) 
						continue;
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
							this.commitSeq);
				}
				
			}*/
			if (msg instanceof CommitRequestMessage) { 
				CommitRequestMessage req = (CommitRequestMessage)msg;
				if(!uniqueCommits.containsKey(req.op.id)) {
					writer.println("Received commit request from "+req.src+" with commit number "+req.senderCommitNumber);
					//writer.flush();
					req.op.commitNumber = commitSeq; 
					commitedOps.add(new PlayListOperation(req.op));
					if(req.op.isCreateOp()) {
						ProcessId ret = new ProcessId(req.op.execStamp.toString()+":"+req.op.execServer);
						System.out.println("In committed.. "+this.me+": Got to know about creation of: "+ret.toString());
						replicas.add(ret);
						versionVector.put(ret,req.op.execStamp);
					} else if (req.op.isRetireOp()) {
						ProcessId ret = req.op.execServer;
						System.out.println("In committed.."+this.me+": Got to know about retirement of: "+ret.toString());
						replicas.remove(ret);
						if(this.connectedReplicas.contains(ret))
							this.connectedReplicas.remove(ret);
						if(versionVector.containsKey(ret))
							versionVector.remove(ret);
					} else if(req.op.isWriteOp()) {
						req.op.operate(commitedPlayList);
						writer.println(this.me+": My committed playlist: "+commitedPlayList.toString());
						//writer.flush();
						versionVector.put(req.op.execServer , req.op.execStamp );
					}
					ProcessId receiver = msg.src; 
					writer.println("Primary replica assigning commit number to "+ req.op.serialize()+" assigned "+commitSeq +" received from "+ receiver);
					//writer.flush();
					commitSeq++;
					for (int i = 0; i < commitSeq ; i ++) {
						CommitResponseMessage commitedMessage = new CommitResponseMessage(new PlayListOperation(commitedOps.get(i)));
						commitedMessage.commitNumber = commitedOps.get(i).commitNumber;
						commitedMessage.src = this.me; 
						commitedMessage.dest = receiver;
						writer.println("Sending commited message "+ commitedOps.get(i).serialize()+" to  "+ commitedMessage.dest);
						sendMessage(receiver, commitedMessage);
					}
					writer.flush();
					uniqueCopies.put(req.op.id,req.op);
					uniqueCommits.put(req.op.id,req.op);
				}
			}else if (msg instanceof RequestMessage) {
				RequestMessage message = (RequestMessage)msg;
				String cmd = message.op;
				PlayListOperation operation = PlayListOperation.getOperation(cmd);
				operation.id = this.me+":"+operation.id;
				operation.execStamp = timeStamp;
				operation.execServer = this.me;
				if(!operation.isWriteOp()){
					/*This is a read op, no need to commit*/
					System.out.println("First printing from committed playlist, \"\" if doesn't exist");
					operation.operate(commitedPlayList);
					System.out.println("Now printing from tentative playlist, \"\" if doesn't exist");
					operation.operate(playList);
					continue;
				}
				ops.add(operation);
				uniqueCopies.put(operation.id, operation);
				operation.operate(playList);
				writer.println(this.me+": My tentative playlist: "+playList.toString());
				versionVector.put(this.me, timeStamp);
				if (ops.size() % entropyThreshold == 0) {
					for (PlayListOperation tempOp : ops) { 
						if (tempOp.commitNumber != -1) {
							System.out.println(this.me +": got op in tentative ops with commit number != -1!");
							continue;
						}
						if(this.connectedReplicas.contains(primary)) {
							CommitRequestMessage commitReq = new CommitRequestMessage(new PlayListOperation(tempOp)); 
							commitReq.src = this.me; 
							commitReq.dest = primary; 
							commitReq.senderCommitNumber = this.commitSeq;
							sendMessage(primary, commitReq);
						}
					}
					for (ProcessId replica : this.connectedReplicas) {
						if (this.me.equals(replica) ) 
							continue;
						AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
								this.commitSeq);
					}
				}
				System.out.println(this.me +" executed "+ operation.serialize());
				writer.flush();
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
					//writer.flush();
					this.commitSeq = (this.commitSeq < message.op.commitNumber) ? message.op.commitNumber : this.commitSeq;
					/*Perform the op*/
					if(message.op.isCreateOp()) {
						ProcessId ret = new ProcessId(message.op.execStamp.toString()+":"+message.op.execServer);
						System.out.println("In tentative.. "+this.me+": Got to know about creation of: "+ret.toString());
						replicas.add(ret);
						versionVector.put(ret,message.op.execStamp);
					}
					else if (message.op.isRetireOp()) {
						ProcessId ret = message.op.execServer;
						System.out.println("In tentative.."+this.me+": Got to know about retirement of: "+ret.toString());
						replicas.remove(ret);
						if(this.connectedReplicas.contains(ret))
							this.connectedReplicas.remove(ret);
						if(versionVector.containsKey(ret))
							versionVector.remove(ret);
					}
					else if(message.op.isWriteOp()) {
						if(message.op.commitNumber == -1) //should always be true.. 
						{
							message.op.operate(playList);
							writer.println(this.me+": My tentative playlist: "+playList.toString());
							versionVector.put(message.op.execServer, message.op.execStamp);
						} else {
							System.out.println("Tentative op with commit number != -1!!");
							writer.println("Tentative op with commit number != -1!!");
						}
					}
					//System.out.println("Reached here..");
					
					if(this.connectedReplicas.contains(primary)) {
						//System.out.println(this.me+": I am connected to primary");
						CommitRequestMessage commitReq = new CommitRequestMessage(new PlayListOperation(message.op)); 
						commitReq.src = this.me; 
						commitReq.dest = primary; 
						commitReq.senderCommitNumber = this.commitSeq;
						sendMessage(primary, commitReq);
					}
					for (ProcessId replica : this.connectedReplicas) {
						if (this.me.equals(replica) ) 
							continue;
						AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
								this.commitSeq);
					}
					//System.out.println(this.me+": Reached here too.., active = "+isActive);
					writer.flush();
					uniqueCopies.put(message.op.id, message.op);
					
				}

			} else if ( msg instanceof CommitResponseMessage) { 
				CommitResponseMessage commitResponse = (CommitResponseMessage)msg;
				//if(this.commitSeq >= commitResponse.commitNumber) // I have already committed this..
				//	continue;
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
					if(commitedOperation.isCreateOp() && !uniqueCopies.containsKey(commitedOperation.id)) {
						ProcessId ret = new ProcessId(commitedOperation.execStamp.toString()+":"+commitedOperation.execServer);
						System.out.println("In committment.. "+this.me+": Got to know about creation of: "+ret.toString());
						replicas.add(ret);
						versionVector.put(ret, commitedOperation.execStamp);
					}
					else if (commitedOperation.isRetireOp() && !uniqueCopies.containsKey(commitedOperation.id)) {
						ProcessId ret = commitedOperation.execServer;
						System.out.println("In committment.. "+this.me+": Got to know about retirement of: "+ret.toString());
						replicas.remove(ret);
						if(this.connectedReplicas.contains(ret))
							this.connectedReplicas.remove(ret);
						if(versionVector.containsKey(ret))
							versionVector.remove(ret);
					}
					if(commitedOperation.isWriteOp() && !commitedOperation.isCreateOp() && !commitedOperation.isRetireOp()) {

						commitedOperation.operate(commitedPlayList);
						writer.println(this.me+": My committed playlist: "+commitedPlayList.toString());
						versionVector.put(commitedOperation.execServer , commitedOperation.execStamp );
					}

					writer.println("Executing commited operation"+commitedOperation.serialize());
					
				}
				for (ProcessId replica : this.connectedReplicas) {
					if (this.me.equals(replica) ) 
						continue;
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
							this.commitSeq);
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
				System.out.println(this.me+ ": will be printing logs...");
				//writer.println(this.me+": In log, My committed playlist: "+commitedPlayList.toString());
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
					writer.println("Showing tentative ops");
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
				writer.println(playList.toString());
				writer.println("Commited PlayList");
				writer.println("###########################");
				writer.println(commitedPlayList.toString());
				writer.flush();
			} else if(msg instanceof CreationMessage) {
				Integer indexOfReplica = ((CreationMessage) msg).indexOfReplica;
				ProcessId ret = new ProcessId(timeStamp.toString()+":"+this.me);
				System.out.println(this.me + ": Received creation write for replica:"+indexOfReplica+"with assigned ID:"+ret.toString());
				replicas.add(ret);
				this.connectedReplicas.add(ret);
				env.replicas.add(ret);
				env.addIdToProc(indexOfReplica, ret);
				Replica repl = new Replica(env, ret, primary);
				repl.setTimeStamp(timeStamp+1);
				repl.setReplicas(replicas);
				List<ProcessId> list = new ArrayList<ProcessId>();
				list.add(me);
				list.add(ret);
				repl.setConnectedList(list);
				//Need to append to write log
				PlayListOperation op = new PlayListOperation();
				op.op = PlayListOperation.OperationTypes.CREATE.value();
				op.execServer = this.me;
				op.execStamp = timeStamp;
				ops.add(op);
				uniqueCopies.put(op.id, op);
				//TODO: need to ensure this write is executed..
				//TODO: do we need to put in version vector??
				//versionVector.put(ret, timeStamp);
				System.out.println("In creation msg.."+this.me + " got to know about, replica:"+indexOfReplica+" with ID:"+ret.toString());
				if(this.connectedReplicas.contains(primary)) {
					CommitRequestMessage commitReq = new CommitRequestMessage(new PlayListOperation(op)); 
					commitReq.src = this.me; 
					commitReq.dest = primary; 
					commitReq.senderCommitNumber = this.commitSeq;
					sendMessage(primary, commitReq);
				}
				for (ProcessId replica : this.connectedReplicas) {
					if (this.me.equals(replica) || replica.equals(primary)) 
						continue;
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
							this.commitSeq);
				}
				timeStamp++;
			} else if(msg instanceof RetirementMessage) { 
				/*I am supposed to retire*/
				System.out.println(this.me+" will be retiring...");
				writer.println("I'll be retiring...");
				int neigh = ((RetirementMessage) msg).neigh;
				ProcessId nhbr = env.idToProc.get(neigh);
				System.out.println("Perform Anti-entropy with:"+nhbr.toString());
				PlayListOperation op = new PlayListOperation();
				op.op = PlayListOperation.OperationTypes.RETIRE.value();
				op.execServer = this.me;
				op.execStamp = timeStamp;
				ops.add(op);
				uniqueCopies.put(op.id, op);
				AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+nhbr),this.me, nhbr, ops, commitedOps,
						this.commitSeq);
				//this.connectedReplicas.clear();
				this.isActive = false;
				writer.flush();
				timeStamp++;
			} else if(msg instanceof BreakConnectionMessage) {
				System.out.println(this.me+": Received break connection message, breaking with "+((BreakConnectionMessage)msg).getNeigh());
				
				ProcessId neigh = env.idToProc.get(((BreakConnectionMessage) msg).getNeigh());
				this.connectedReplicas.remove(neigh);
				if(replicas.contains(neigh))
					replicas.remove(neigh);
				for(ProcessId replica : this.connectedReplicas) {
					writer.println(this.me+": connected to "+replica.toString());
					
				}
				writer.println("#####################");
				writer.flush();
			} else if(msg instanceof RecoverConnectionMessage) {
				System.out.println(this.me+": Received recover connection message");
				ProcessId neigh = env.idToProc.get(((RecoverConnectionMessage) msg).getNeigh());
				this.connectedReplicas.add(neigh);
				if(!replicas.contains(neigh))
					replicas.add(neigh);
				for(ProcessId replica : this.connectedReplicas) {
					writer.println(this.me+": connected to "+replica.toString());
					
					
				}
				for (ProcessId replica : this.connectedReplicas) {
					if (this.me.equals(replica) ) 
						continue;
					AntiEntropy ae = new AntiEntropy(this.env, new ProcessId(this.me+":antiEntropy"+replica),this.me, replica, ops, commitedOps,
							this.commitSeq);
				}
				writer.println("#####################");
				writer.flush();
			}
			
		}

	}
}
