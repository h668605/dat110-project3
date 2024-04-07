package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {


	public static BigInteger hashOf(String entity) {

		BigInteger hashint = null;

		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			// Compute the hash of the input 'entity'
			byte[] hashBytes = md.digest(entity.getBytes());

			// Convert the hash into hex format using the provided toHex() method
			String hashString = toHex(hashBytes);

			// Convert the hex into BigInteger
			hashint = new BigInteger(hashString, 16);

		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		// Return the BigInteger
		return hashint;
	}
	
	public static BigInteger addressSize() {
		
		// Task: compute the address size of MD5
		// compute the number of bits = bitSize()
		int numberOfBits = bitSize();
		// compute the address size = 2 ^ number of bits
		BigInteger addressSize = BigInteger.valueOf(2).pow(numberOfBits);
		// return the address size
		return addressSize;

	}
	
	public static int bitSize() {

		int digestlen = 16; // MD5 digest length in bytes
		return digestlen * 8; // Convert bytes to bits
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
