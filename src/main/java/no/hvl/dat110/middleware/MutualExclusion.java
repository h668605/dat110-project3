/**
 *
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {

	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource) 
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private List<Message> queueack; 						// queue for acknowledged messages
	private List<Message> mutexqueue;						// queue for storing process that are denied permission. We really don't need this for quorum-protocol

	private LamportClock clock;								// lamport clock
	private Node node;

	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;

		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}

	public synchronized void acquireLock() {
		CS_BUSY = true;
	}

	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
		// Log that the node wants to access the critical section
		logger.info(node.nodename + " wants to access CS");

		// Clear the queues before starting the mutual exclusion algorithm
		queueack.clear();
		mutexqueue.clear();

		// Increment the Lamport clock
		clock.increment();

		// Set the clock on the message
		message.setClock(clock.getClock());

		// Indicate that the node wants to access the resource
		WANTS_TO_ENTER_CS = true;

		// Remove duplicates from the list of active nodes
		List<Message> uniqueActiveNodes = removeDuplicatePeersBeforeVoting();

		// Multicast the message to the active nodes
		multicastMessage(message, uniqueActiveNodes);

		// Wait for acknowledgements from all active nodes
		if (areAllMessagesReturned(uniqueActiveNodes.size())) {
			// Acquire the lock
			acquireLock();

			// Broadcast updates to all peers
			node.broadcastUpdatetoPeers(updates);

			// Clear the mutexqueue
			mutexqueue.clear();

			// Return permission to enter the critical section
			return true;
		}

		// Permission to enter the critical section was denied
		return false;
	}

	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
		logger.info("Number of peers to vote = " + activenodes.size());

		// Iterate over the active nodes
		for (Message peer : activenodes) {
			try {
				// Obtain a stub for each node from the registry
				NodeInterface nodeStub = Util.getProcessStub(peer.getNodeName(), peer.getPort());

				// Call onMutexRequestReceived on the node's stub
				nodeStub.onMutexRequestReceived(message);
			} catch (Exception e) {
				logger.error("Error multicasting message to " + peer.getNodeName() + ": " + e.getMessage());
			}
		}
	}
	public void onMutexRequestReceived(Message message) throws RemoteException {
		// if message is from self, acknowledge, and call onMutexAcknowledgementReceived()
		if (message.getNodeName().equals(node.nodename)) {
			message.setAcknowledged(true);
			onMutexAcknowledgementReceived(message);
		}

		int decisionCase = (!CS_BUSY && !WANTS_TO_ENTER_CS) ? 0 : CS_BUSY ? 1 : 2;

		doDecisionAlgorithm(message, mutexqueue, decisionCase);
	}

	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		String procName = message.getNodeName();
		int port = message.getPort();

		switch (condition) {
			// Case 0: Receiver is not accessing shared resource and does not want to
			case 0: {
				// Get a stub for the sender from the registry
				NodeInterface senderStub = Util.getProcessStub(procName, port);

				// Acknowledge message
				logger.info("Acknowledging mutex request from " + procName);
				senderStub.onMutexAcknowledgementReceived(message);

				break;
			}
			// Case 1: Receiver already has access to the resource
			case 1: {
				// Queue this message
				queue.add(message);
				logger.info("Queued mutex request from " + procName);
				break;
			}
			// Case 2: Receiver wants to access resource but is yet to
			case 2: {
				// Check the clock of the sending process (in the received message)
				int senderClock = message.getClock();

				// Own clock of the receiver (in the node's message)
				int receiverClock = clock.getClock();

				// Compare clocks
				if (senderClock < receiverClock || (senderClock == receiverClock && procName.compareTo(node.nodename) < 0)) {
					// Acknowledge the message
					logger.info("Acknowledging mutex request from " + procName);
					NodeInterface senderStub = Util.getProcessStub(procName, port);
					senderStub.onMutexAcknowledgementReceived(message);
				} else {
					// Queue the message
					queue.add(message);
					logger.info("Queued mutex request from " + procName);
				}
				break;
			}
			default:
				break;
		}
	}

	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
		// add message to queueack
		queueack.add(message);
	}

	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) {
		logger.info("Releasing locks from = " + activenodes.size());

		// Iterate over the active nodes
		for (Message node : activenodes) {
			try {
				// Obtain a stub for each node from the registry
				NodeInterface nodeStub = Util.getProcessStub(node.getNodeName(), node.getPort());

				// Call releaseLocks on the node's stub
				nodeStub.releaseLocks();
				logger.info("Locks released for " + node.getNodeName());
			} catch (Exception e) {
				logger.error("Error releasing locks for " + node.getNodeName() + ": " + e.getMessage());
			}
		}
	}

	private boolean areAllMessagesReturned(int numVoters) throws RemoteException {
		logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());

		// check if the size of the queueack is same as the numVoters
		// clear the queueack
		// return true if yes and false if no
		if (queueack.size() == numVoters) {
			queueack.clear();
			return true;
		}

		return false;
	}

	private List<Message> removeDuplicatePeersBeforeVoting() {
		List<Message> uniquePeer = new ArrayList<>();

		// iterate over active nodes for file
		for (Message p : node.activenodesforfile) {
			// check if node is already in uniquePeer list
			if (uniquePeer.stream().noneMatch(p1 -> p.getNodeName().equals(p1.getNodeName()))) {
				uniquePeer.add(p);
			}
		}

		return uniquePeer;
	}
}