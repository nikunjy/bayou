package bayou.entities;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.corba.se.pept.transport.Acceptor;

public class Env {
	Map<ProcessId, Process> procs = new HashMap<ProcessId, Process>();
	List<ProcessId> replicas;
	ProcessId primary;
	public final static int nReplicas = 4;
	private int numClients;
	class BlackList {
		String process1; 
		String process2;
		public BlackList(String p1, String p2)  {
			this.process1 = p1; 
			this.process2 = p2;
		}
	};
	public List<BlackList> blackList = new ArrayList<BlackList>();
	public Env() { 
		try {
			BufferedReader br = new BufferedReader(new FileReader("BlackList.txt"));
			String line = "";
			while ((line = br.readLine()) != null) {
				String p[] = line.split(" ");
				System.out.println("Adding to blacklist "+p[0]+" "+p[1]);
				blackList.add(new BlackList(p[0],p[1]));
			}
		} catch(Exception e) { 
			
		}
	}
	boolean isBlackListMessage(ProcessId dst, BayouMessage msg) {
		if (msg.src == null || dst == null) { 
			return false;
		}
		
		for (BlackList item : blackList) { 
			if (dst.name.contains(item.process1) && msg.src.name.contains(item.process2)) { 
				return true;
			}
			if (dst.name.contains(item.process2) && msg.src.name.contains(item.process1)) { 
				return true;
			}
		}
		return false;
	}
	synchronized void sendMessage(ProcessId dst, BayouMessage msg){
		Process p = procs.get(dst);
		if (p != null) {
			p.deliver(msg);
		}
	}
	synchronized void addProc(ProcessId pid, Process proc){
		procs.put(pid, proc);
		proc.start();
	}
	synchronized void removeProc(ProcessId pid){
		if (procs.containsKey(pid)) { 
			procs.remove(pid);
			return;
		}
		for (ProcessId id : procs.keySet()) {
			if (id.name.equalsIgnoreCase(pid.name)) {
				procs.remove(id);
				System.out.println("Killed "+id);
				for (ProcessId tid : procs.keySet()) {
					System.out.println(tid);
				}
				break;
			}
		}
	}
	public enum UserCommandTypes {
		KILLPROCESS("killProcess:"),DELPARTITION("deletePartition:"),ADDBLACKLIST("add:"),COMMAND("command:");
		public String message;
		public String value() { 
			return message;
		}
		UserCommandTypes(String message) {
			this.message = message;
		}
	}
	class UserReader extends Process {
		public UserReader(Env env,ProcessId pid) { 
			this.env = env;
			this.me = pid;
			env.addProc(pid, this);	
		}
		public void body() { 
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				Thread.sleep(7000);
				while(true) {
				System.out.println("Enter your Command : ");
				String input  = br.readLine();
				for (int i=0;i<UserCommandTypes.values().length;i++) { 
					UserCommandTypes type = UserCommandTypes.values()[i];
					if (input.contains(type.value())) { 
						if (type.equals(UserCommandTypes.KILLPROCESS)) {
							String id = input.substring(input.indexOf(":")+1);
							System.out.println(id);
							env.removeProc(new ProcessId(id));
						} else if (type.equals(UserCommandTypes.DELPARTITION))  {
							env.blackList.clear();
						} else if(type.equals(UserCommandTypes.ADDBLACKLIST)) {
							String in = input.substring(input.indexOf(":")+1);
							String p[] = in.split(" ");
							System.out.println("Adding to blacklist "+p[0]+" "+p[1]);
							env.blackList.add(new BlackList(p[0],p[1]));
							
						} else if (type.equals(UserCommandTypes.COMMAND)) { 
							String command = input.substring(input.indexOf(":")+1);
							System.out.println(command);
							for (int r=0;r<env.replicas.length;r++) {
							sendMessage(env.replicas[r],
									new RequestMessage(this.me, new Command(this.me, 0,command)));
							}
						}
					}
				}
				}
			}catch(Exception e) { 
				System.out.println(e);
			}
		}
	}
	class Client extends Process {
		public ProcessId pid;
		public int clientId;
		public Client(Env env,ProcessId pid, int clientId) {
			this.env = env; 
			this.clientId = clientId;
			this.pid = pid;
			this.me = pid;
			env.addProc(pid, this);
		}
		public void body() { 
			try {
				BufferedReader br = new BufferedReader(new FileReader(clientId+".txt"));
				String line = "";
				while ((line = br.readLine()) != null) {
					System.out.println("Client "+clientId +" executing "+line);
					if (line.contains("delay:")) {
						line = line.substring(line.indexOf(":")+1);
						long sleepTime = Long.parseLong(line);  
						Thread.sleep(sleepTime);
					} else {
						for (int r = 0; r < nReplicas; r++) {
							sendMessage(env.replicas.get(r),new RequestMessage(pid, new Command(pid, 0,line)));
						}	
					}
				}
			} catch(Exception e) {
			}finally {
			}
		}
	};
	void run(String[] args){
		replicas = new ArrayList<ProcessId>();
		primary = new ProcessId("primary-replica");
		for (int i = 0; i < nReplicas; i++) {
			replicas.add(new ProcessId("replica:" + i));
			Replica repl = new Replica(this, replicas.get(i), primary);
			repl.setReplicas(replicas);
		}
		if (numClients == 1) {
			for (int i = 1; i < 10; i++) {
				ProcessId pid = new ProcessId("client:" + i);
				BankOperation op = new BankOperation();
				op.op = BankOperation.OperationTypes.ADDACCOUNT.value();
				op.holderName = "Client "+i;
				op.amount = 500;
				for (int r = 0; r < nReplicas; r++) {
					sendMessage(replicas[r],
							new RequestMessage(pid, new Command(pid, 0, op.serialize())));
				}
			}
		} else { 
			for (int i = 1; i <= numClients ; i++) { 
				ProcessId pid = new ProcessId("client:" + i);
				System.out.println("starting client"+i);
				Client c = new Client(this,pid,i); 
				//((Thread)c).start();
			}
		}
		UserReader ucmd = new UserReader(this,new ProcessId("usercmd:"));
	}

	public static void main(String[] args){
		Env obj = new Env();
		if (args.length>0) { 
			obj.numClients = Integer.parseInt(args[0]);
		} else { 
			obj.numClients = 1;
		}
		obj.run(args);
	}
}

