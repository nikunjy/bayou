package bayou.entities;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import bayou.types.PlayList;
import bayou.types.PlayListOperation;


public class Replica extends Process {
	List<ProcessId> replicas;
	PlayList playList;
	List<PlayListOperation> ops;
	ProcessId primary;
	PrintWriter writer;
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
		env.addProc(me, this);
	}
	public void setReplicas(List<ProcessId> replicas) {
		 this.replicas = replicas;
	}
	@Override
	void body() {
		for (;;) {
			BayouMessage msg = getNextMessage();
			if (msg instanceof RequestMessage) {
				RequestMessage message = (RequestMessage)msg;
				String cmd = message.op; 
			}
		}
		
	}
}
