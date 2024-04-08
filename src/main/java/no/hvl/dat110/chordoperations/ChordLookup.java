/**
 * 
 */
package no.hvl.dat110.chordoperations;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class ChordLookup {

	private static final Logger logger = LogManager.getLogger(ChordLookup.class);
	private Node node;
	
	public ChordLookup(Node node) {
		this.node = node;
	}

	public NodeInterface findSuccessor(BigInteger key) throws RemoteException {
		try {
			// Ask this node to find the successor of the key
			NodeInterface succNode = node.getSuccessor();

			// Check if key is a member of the set {nodeid+1, ..., succID}
			if (Util.checkInterval(key, node.getNodeID().add(BigInteger.ONE), succNode.getNodeID())) {
				// Key belongs to the interval, return the successor
				return succNode;
			} else {
				// Key does not belong to the interval, find highest predecessor
				NodeInterface highestPred = findHighestPredecessor(key);

				if (highestPred != null && !highestPred.getNodeID().equals(node.getNodeID())) {
					// Ask the highest predecessor to find the successor of the key
					return highestPred.findSuccessor(key);
				}
			}
		} catch (RemoteException e) {
			// Handle RemoteException
		}

		return null;
	}





	/**
	 * This method makes a remote call. Invoked from a local client
	 * @param ID BigInteger
	 * @return
	 * @throws RemoteException
	 */
	private NodeInterface findHighestPredecessor(BigInteger ID) throws RemoteException {
		// Collect the entries in the finger table for this node
		List<NodeInterface> fingerTable = node.getFingerTable();

		// Starting from the last entry, iterate over the finger table
		for (int i = fingerTable.size() - 1; i >= 0; i--) {
			NodeInterface fingernode = fingerTable.get(i);

			// Check if finger is a member of the set {nodeID+1, ..., ID-1}
			if (Util.checkInterval(fingernode.getNodeID(), node.getNodeID().add(BigInteger.ONE), ID.subtract(BigInteger.ONE))) {
				return fingernode; // Return the finger as the highest predecessor
			}
		}

		return null; // If no closest predecessor is found, return null
	}


	public void copyKeysFromSuccessor(NodeInterface succ) {
		
		Set<BigInteger> filekeys;
		try {
			// if this node and succ are the same, don't do anything
			if(succ.getNodeName().equals(node.getNodeName()))
				return;
			
			logger.info("copy file keys that are <= "+node.getNodeName()+" from successor "+ succ.getNodeName()+" to "+node.getNodeName());
			
			filekeys = new HashSet<>(succ.getNodeKeys());
			BigInteger nodeID = node.getNodeID();
			
			for(BigInteger fileID : filekeys) {

				if(fileID.compareTo(nodeID) <= 0) {
					logger.info("fileID="+fileID+" | nodeID= "+nodeID);
					node.addKey(fileID); 															// re-assign file to this successor node
					Message msg = succ.getFilesMetadata().get(fileID);				
					node.saveFileContent(msg.getNameOfFile(), fileID, msg.getBytesOfFile(), msg.isPrimaryServer()); 			// save the file in memory of the newly joined node
					succ.removeKey(fileID); 	 																				// remove the file key from the successor
					succ.getFilesMetadata().remove(fileID); 																	// also remove the saved file from memory
				}
			}
			
			logger.info("Finished copying file keys from successor "+ succ.getNodeName()+" to "+node.getNodeName());
		} catch (RemoteException e) {
			logger.error(e.getMessage());
		}
	}

	public void notify(NodeInterface pred_new) throws RemoteException {
		
		NodeInterface pred_old = node.getPredecessor();
		
		// if the predecessor is null accept the new predecessor
		if(pred_old == null) {
			node.setPredecessor(pred_new);		// accept the new predecessor
			return;
		}
		
		else if(pred_new.getNodeName().equals(node.getNodeName())) {
			node.setPredecessor(null);
			return;
		} else {
			BigInteger nodeID = node.getNodeID();
			BigInteger pred_oldID = pred_old.getNodeID();
			
			BigInteger pred_newID = pred_new.getNodeID();
			
			// check that pred_new is between pred_old and this node, accept pred_new as the new predecessor
			// check that ftsuccID is a member of the set {nodeID+1,...,ID-1}
			boolean cond = Util.checkInterval(pred_newID, pred_oldID.add(BigInteger.ONE), nodeID.add(BigInteger.ONE));
			if(cond) {		
				node.setPredecessor(pred_new);		// accept the new predecessor
			}	
		}		
	}

}
